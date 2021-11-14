package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/flow/go/shuffle"
	"github.com/estuary/protocols/catalog"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

type matState int

/*
matLoadCommitSent:
  On consumeCh, combine, track, and send Load.
	On rx Loaded, reduce.
	On rx DriverCommitted:
		- Resolve prior driverCommittedOp (last used as a log write dependency; this unblocks logCommittedOp)
		- Transition to matLoadDriverCommitted.

matLoadDriverCommitted:
  On consumeCh, combine, track, and send Load.
	On rx Loaded, reduce.
	On logCommittedOp:
	  - Nil logCommittedOp.
		- Send Acknowledge.
		- Transition to matLoadAckSent.

matLoadAckSent:
  On consumeCh, combine, track, and send Load.
	On rx Loaded, reduce.
	On rx Acknowledged:
	  - Resolve and nil acknowledgedOp (this unblocks consumer.runTransactions barrier).
		- Transition to matLoadAcknowledged

matLoadAcknowledged:
  On consumeCh, combine, track, and send Load.
	On rx Loaded, reduce.
	On startCommitCh:
		- Send Prepare.
		- Transition to matLoadPrepSent.

matLoadPrepSent:
	On rx Loaded, reduce.
	On rx Prepared:
		- Init acknowledgedOp.
	  - Send to drainCh with driver checkpoint, driverTx, flighted, combiners to drain, and owned acknowledgedOp.
		  (This is being synchronously waited on by StartCommit. StartCommit drains the combiner, and starts
				a recovery log commit which awaits acknowledgedOp).
		- Transition to matStore.

matStore:
	On recv drainedCh:
	  - Set logCommittedOp, driverTx, and flighted.
		- Re-init combiners.
		- Send Commit.
		- Transition to matLoadCommitSent.

*/
const (
	matInit                matState = iota
	matConnected           matState = iota
	matOpened              matState = iota
	matLoadCommitSent      matState = iota
	matLoadDriverCommitted matState = iota
	matLoadAckSent         matState = iota
	matLoadAcknowledged    matState = iota
	matLoadPrepSent        matState = iota
	matStore               matState = iota
)

type matFSM struct {
	state matState
	// doneCh is closed upon fsmLoop's return.
	doneCh chan struct{}
	// loopErr is the terminal error of fsmLoop, readable upon |doneCh|.
	loopErr error

	// Materialization specification.
	materialization *pf.MaterializationSpec
	// Driver responses, exclusively read by fsmLoop.
	driverRx <-chan materialize.TransactionResponse
	// Owned transaction state.
	txn *matTxn

	// Operation which fsmLoop resolves on DriverCommitted of the prior transaction.
	// This operation was used by StartCommit as a dependency of its recovery
	// log write, and resolving it allows the recovery log commit to proceed.
	// Nil if there isn't an ongoing driver commit.
	driverCommittedOp *client.AsyncOperation
	// Operation which resolves on the prior transactions's commit to its recovery log.
	// When signaled, fsmLoop notifies the driver via Acknowledge.
	// Nil if there isn't an ongoing recovery log commit.
	logCommittedOp client.OpFuture
	// Operation which fsmLoop resolves on driver Acknowledged of the
	// prior transaction's recovery log commit.
	// This operation was returned by StartCommit and notifies the Gazette consumer
	// transaction machinery that it may begin to close a current transaction.
	// Nil if there isn't an ongoing driver acknowledgement.
	acknowledgedOp *client.AsyncOperation

	// Sends from RestoreCheckpoint to initiate a shutdown.
	shutdownCh chan struct{}

	// Sends transaction documents from ConsumeMessage to fsmLoop.
	consumeCh chan pf.IndexedShuffleResponse
	// Sends Flow checkpoint from StartCommit to the fsmLoop.
	startCommitCh chan pf.Checkpoint
	// Sends a drain capability from the fsmLoop to StartCommit.
	drainCh chan matDrain
	// Sends an exhausted drain and log commit operation from StartCommit to the fsmLoop.
	drainedCh chan matDrained
}

// matTxn is the state of a current transaction.
// It's passed between goroutines curing the course of a transaction
// but is always exclusively held by just one goroutine.
type matTxn struct {
	// Combiners of the materialization, one for each binding.
	combiners []*bindings.Combine
	// Flighted keys of the current transaction for each binding, plus a bounded number of
	// retained fully-reduced documents of the last transaction.
	flighted []map[string]json.RawMessage
	// Capability to send requests to the driver.
	tx pm.Driver_TransactionsClient
	// Next staged request sent to the driver.
	txStaged *pm.TransactionRequest
}

func (t *matTxn) destroy() {
	for _, c := range t.combiners {
		c.Destroy()
	}
	_ = t.tx.CloseSend()
}

// matDrain is an owned capability to drain a materialization transaction.
type matDrain struct {
	fsm      *matFSM                         // FSM which initiated the drain.
	txn      *matTxn                         // Owned transaction state.
	prepared pm.TransactionResponse_Prepared // Driver's prepared checkpoint.
}

// matDrained is conceptually a return of a capability to drain a
// materialization transaction, extended with an error status and
// a logCommittedOp which may resolve on a future recovery log commit.
type matDrained struct {
	err            error           // Error encountered while draining, if any.
	logCommittedOp client.OpFuture // Operation which will commit to the recovery log.
	txn            *matTxn         // Owned transaction state.
}

func newMatFSM(
	fqn string,
	spec *pf.MaterializationSpec,
	schemaIndex *bindings.SchemaIndex,
	logger ops.Logger,
) (*matFSM, error) {

	var combiners []*bindings.Combine
	var flighted []map[string]json.RawMessage

	for i, b := range spec.Bindings {
		combiner, err := bindings.NewCombine(logger)
		if err != nil {
			return nil, fmt.Errorf("creating combiner: %w", err)
		}
		combiners = append(combiners, combiner)
		flighted = append(flighted, make(map[string]json.RawMessage))

		if err = combiners[i].Configure(
			fqn,
			schemaIndex,
			b.Collection.Collection,
			b.Collection.SchemaUri,
			"", // Don't generate UUID placeholders.
			b.Collection.KeyPtrs,
			b.FieldValuePtrs(),
		); err != nil {
			return nil, fmt.Errorf("building combiner: %w", err)
		}
	}

	return &matFSM{
		state:           matLoadAcknowledged,
		doneCh:          make(chan struct{}),
		loopErr:         nil,
		materialization: spec,
		txn: &matTxn{
			combiners: combiners,
			flighted:  flighted,
			txStaged:  new(pm.TransactionRequest),
			tx:        nil, // Initialized on connect.
		},
	}, nil
}

func (fsm *matFSM) connect(ctx context.Context, network string, logger ops.Logger) error {
	if fsm.state != matInit {
		panic(fsm.state)
	}

	// Establish driver connection and start Transactions RPC.
	conn, err := materialize.NewDriver(ctx,
		fsm.materialization.EndpointType,
		fsm.materialization.EndpointSpecJson,
		network,
		logger,
	)
	if err != nil {
		return fmt.Errorf("building endpoint driver: %w", err)
	}
	fsm.txn.tx, err = conn.Transactions(ctx)
	if err != nil {
		return fmt.Errorf("driver.Transactions: %w", err)
	}
	fsm.driverRx = materialize.TransactionResponseChannel(fsm.txn.tx)

	fsm.state = matConnected
	return nil
}

func (fsm *matFSM) open(
	labeling labels.ShardLabeling,
	driverCheckpoint json.RawMessage,
) (*pm.TransactionResponse_Opened, error) {

	if fsm.state != matConnected {
		panic(fsm.state)
	}

	if err := pm.WriteOpen(
		fsm.txn.tx,
		&fsm.txn.txStaged,
		fsm.materialization,
		labeling.Build,
		&labeling.Range,
		driverCheckpoint,
	); err != nil {
		return nil, err
	}

	// Read Opened response with driver's optional Flow Checkpoint.
	opened, err := materialize.Rx(fsm.driverRx, true)
	if err != nil {
		return nil, fmt.Errorf("reading Opened: %w", err)
	} else if opened.Opened == nil {
		return nil, fmt.Errorf("expected Opened, got %#v", opened.String())
	}

	fsm.state = matOpened
	return opened.Opened, nil
}

func (fsm *matFSM) reAcknowledge() error {
	if fsm.state != matOpened {
		panic(fsm.state)
	}

	// Write Acknowledge request to re-acknowledge the last commit.
	if err := pm.WriteAcknowledge(fsm.txn.tx, &fsm.txn.txStaged); err != nil {
		return err
	}

	// Read Acknowledged response.
	acked, err := materialize.Rx(fsm.driverRx, true)
	if err != nil {
		return fmt.Errorf("reading Acknowledged: %w", err)
	} else if acked.Acknowledged == nil {
		return fmt.Errorf("expected Acknowledged, got %#v", acked.String())
	}

	fsm.state = matLoadAcknowledged
	return nil
}

func (fsm *matFSM) fsmLoop() (__out error) {
	if fsm.state != matLoadAcknowledged {
		panic(fsm.state)
	}

	defer func() {
		fsm.loopErr = __out
		close(fsm.doneCh)

		// These can never succeed, since we're no longer servicing driverRx.
		if fsm.driverCommittedOp != nil {
			fsm.driverCommittedOp.Resolve(fsm.loopErr)
		}
		if fsm.acknowledgedOp != nil {
			fsm.acknowledgedOp.Resolve(fsm.loopErr)
		}
		if fsm.txn != nil {
			fsm.txn.destroy()
		}
	}()

	for {
		var maybeLogCommitted <-chan struct{}
		if fsm.logCommittedOp != nil {
			maybeLogCommitted = fsm.logCommittedOp.Done()
		}

		select {
		case doc := <-fsm.consumeCh:
			if err := fsm.onConsume(doc); err != nil {
				return fmt.Errorf("onConsume: %w", err)
			}
		case prepare := <-fsm.startCommitCh:
			if err := fsm.onStartCommit(prepare); err != nil {
				return fmt.Errorf("onStartCommit: %w", err)
			}
		case drained := <-fsm.drainedCh:
			if err := fsm.onDrained(drained); err != nil {
				return fmt.Errorf("onDrained: %w", err)
			}
		case <-maybeLogCommitted:
			if err := fsm.onLogCommitted(); err != nil {
				return fmt.Errorf("onLogCommitted: %w", err)
			}
		case <-fsm.shutdownCh:
			if err := fsm.onShutdown(); err != nil {
				return fmt.Errorf("onShutdown: %w", err)
			}

		case rx, ok := <-fsm.driverRx:
			if !ok {
				return io.EOF
			} else if rx.Error != nil {
				return rx.Error
			} else if err := rx.Validate(); err != nil {
				return err
			}

			switch {
			case rx.Loaded != nil:
				if err := fsm.onLoaded(*rx.Loaded); err != nil {
					return fmt.Errorf("onLoaded: %w", err)
				}
			case rx.Acknowledged != nil:
				if err := fsm.onAcknowledged(*rx.Acknowledged); err != nil {
					return fmt.Errorf("onAcknowledged: %w", err)
				}
			case rx.Prepared != nil:
				if err := fsm.onPrepared(*rx.Prepared); err != nil {
					return fmt.Errorf("onPrepared: %w", err)
				}
			case rx.DriverCommitted != nil:
				if err := fsm.onDriverCommitted(*rx.DriverCommitted); err != nil {
					return fmt.Errorf("onDriverCommitted: %w", err)
				}
			default:
				panic(fmt.Sprint(rx))
			}

		}
	}
	return nil
}

func (fsm *matFSM) onConsume(doc pf.IndexedShuffleResponse) error {
	switch fsm.state {
	case matLoadCommitSent, matLoadDriverCommitted, matLoadAckSent, matLoadAcknowledged:
		// Pass.
	default:
		panic(fsm.state) // This is a runtime implementation error.
	}

	// Find *Shuffle with equal pointer.
	var binding = -1 // Panic if no *Shuffle is matched.
	var flighted map[string]json.RawMessage
	var combiner *bindings.Combine
	var deltaUpdates bool

	for i := range fsm.materialization.Bindings {
		if &fsm.materialization.Bindings[i].Shuffle == doc.Shuffle {
			binding = i
			flighted = fsm.txn.flighted[i]
			combiner = fsm.txn.combiners[i]
			deltaUpdates = fsm.materialization.Bindings[i].DeltaUpdates
		}
	}

	var packedKey = doc.Arena.Bytes(doc.PackedKey[doc.Index])

	// Note the Go compiler is clever enough to avoid allocation
	// when a use of string([]byte) is inlined into a map lookup.

	if doc, ok := flighted[string(packedKey)]; ok && doc == nil {
		// We've already seen this key within this transaction.
	} else if ok {
		// We retained this document from the last transaction.
		if deltaUpdates {
			panic("we shouldn't have retained if deltaUpdates")
		}
		if err := combiner.ReduceLeft(doc); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
		flighted[string(packedKey)] = nil // Clear old value & mark as visited.
	} else {
		// This is a novel key.
		if !deltaUpdates {
			if err := pm.StageLoad(fsm.txn.tx, &fsm.txn.txStaged, binding, packedKey); err != nil {
				return err
			}
		}
		flighted[string(packedKey)] = nil // Mark as visited.
	}

	if err := combiner.CombineRight(doc.Arena.Bytes(doc.DocsJson[doc.Index])); err != nil {
		return fmt.Errorf("combiner.CombineRight: %w", err)
	}

	// fsm.state is unchanged.
	return nil
}

func (fsm *matFSM) onLoaded(loaded pm.TransactionResponse_Loaded) error {
	switch fsm.state {
	case matLoadDriverCommitted, matLoadAckSent, matLoadAcknowledged, matLoadPrepSent:
		// Pass.
	default:
		return fmt.Errorf("protocol error (Loaded not expected in state %v)", fsm.state)
	}

	if int(loaded.Binding) > len(fsm.txn.combiners) {
		return fmt.Errorf("driver error (binding %d out of range)", loaded.Binding)
	}

	// Feed documents into the combiner as reduce-left operations.
	var combiner = fsm.txn.combiners[loaded.Binding]
	for _, slice := range loaded.DocsJson {
		if err := combiner.ReduceLeft(loaded.Arena.Bytes(slice)); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
	}

	// fsm.state is unchanged.
	return nil
}

func (fsm *matFSM) onAcknowledged(pm.TransactionResponse_Acknowledged) error {
	switch fsm.state {
	case matLoadAckSent:
		// Pass.
	default:
		return fmt.Errorf("protocol error (Acknowledged not expected in state %v)", fsm.state)
	}

	// This future was returned by StartCommit of the last transaction.
	// Gazette holds the current transaction open until it resolves.
	fsm.acknowledgedOp.Resolve(nil)
	fsm.acknowledgedOp = nil

	fsm.state = matLoadAcknowledged
	return nil
}

func (fsm *matFSM) onStartCommit(cp pf.Checkpoint) error {
	switch fsm.state {
	case matLoadAcknowledged:
		// Pass.
	default:
		panic(fsm.state) // This is a runtime implementation error.
	}

	// Write our intent to close the transaction and prepare for commit.
	// This signals the driver to send remaining Loaded responses, if any.
	if err := pm.WritePrepare(fsm.txn.tx, &fsm.txn.txStaged, cp); err != nil {
		return err
	}

	fsm.state = matLoadPrepSent
	return nil
}

func (fsm *matFSM) onPrepared(prepared pm.TransactionResponse_Prepared) error {
	switch fsm.state {
	case matLoadPrepSent:
		// Pass.
	default:
		return fmt.Errorf("protocol error (Prepared not expected in state %v)", fsm.state)
	}

	// Future we'll resolve upon a future DriverCommitted.
	fsm.driverCommittedOp = client.NewAsyncOperation()
	// Future we'll resolve upon a future driver Acknowledged.
	fsm.acknowledgedOp = client.NewAsyncOperation()

	// A concurrent call to StartCommit is waiting for a synchronous send on fsm.drainCh.
	// Give it an exclusively-owned capability to drain combiners to the driver.
	var drain = matDrain{
		fsm:      fsm,
		txn:      fsm.txn,
		prepared: prepared,
	}
	fsm.txn = nil

	fsm.drainCh <- drain

	fsm.state = matStore
	return nil
}

func (d matDrain) do(logCommittedOp client.OpFuture) (__out error) {
	defer func() {
		select {
		case d.fsm.drainedCh <- matDrained{
			err:            __out,
			logCommittedOp: logCommittedOp,
			txn:            d.txn,
		}: // Pass
		case <-d.fsm.doneCh:
			d.txn.destroy()
		}
	}()

	// Drain each binding.
	for i, combiner := range d.txn.combiners {
		if err := drainBinding(
			d.txn.flighted[i],
			combiner,
			d.fsm.materialization.Bindings[i].DeltaUpdates,
			d.txn.tx,
			&d.txn.txStaged,
			i,
		); err != nil {
			return err
		}
	}

	return nil
}

func (fsm *matFSM) onDrained(drained matDrained) error {
	if fsm.state != matStore {
		panic(fsm.state) // This is a runtime implementation error.
	}
	fsm.txn = drained.txn

	if drained.err != nil {
		return drained.err
	}

	// Track the log commit operation for it's expected future resolution.
	fsm.logCommittedOp = drained.logCommittedOp

	// Tell the driver to commit.
	if err := pm.WriteCommit(fsm.txn.tx, &fsm.txn.txStaged); err != nil {
		return err
	}

	fsm.state = matLoadCommitSent
	return nil
}

func (fsm *matFSM) onDriverCommitted(pm.TransactionResponse_DriverCommitted) error {
	if fsm.state != matLoadCommitSent {
		return fmt.Errorf("protocol error (DriverCommitted not expected in state %v)", fsm.state)
	}

	// This future was used as a recovery log write dependency by StartCommit of
	// the last transaction. Resolving it allows that recovery log write to proceed,
	// which we'll observe as a future resolution of fsm.logCommittedOp.
	fsm.driverCommittedOp.Resolve(nil)
	fsm.driverCommittedOp = nil

	if fsm.logCommittedOp == nil {
		panic("expected fsm.logCommittedOp to be !nil")
	}

	fsm.state = matLoadDriverCommitted
	return nil
}

func (fsm *matFSM) onLogCommitted() error {
	switch fsm.state {
	case matLoadDriverCommitted:
		// Pass.
	default:
		panic(fsm.state) // This is a runtime implementation error.
	}

	fsm.logCommittedOp = nil

	// Tell the driver to acknowledge.
	if err := pm.WriteAcknowledge(fsm.txn.tx, &fsm.txn.txStaged); err != nil {
		return err
	}

	fsm.state = matLoadAckSent
	return nil
}

func (fsm *matFSM) onShutdown() error {
	if fsm.state != matLoadAcknowledged {
		panic(fsm.state)
	}

	if err := fsm.txn.tx.CloseSend(); err != nil {
		return err
	}

	return nil
}

// Materialize is a top-level Application which implements the materialization workflow.
type Materialize struct {
	// Coordinator of shuffled reads for this materialization shard.
	coordinator *shuffle.Coordinator
	// FlowConsumer which owns this Materialize shard.
	host *FlowConsumer
	// Transaction state shared with the concurrent read loop.
	fsm *matFSM
	// Store delegate for persisting local checkpoints.
	store connectorStore
	// Embedded task reader scoped to current task version.
	// Initialized in RestoreCheckpoint.
	taskReader
	// Embedded processing state scoped to a current task version.
	// Initialized in RestoreCheckpoint.
	taskTerm
}

var _ Application = (*Materialize)(nil)

// NewMaterializeApp returns a new Materialize, which implements Application.
func NewMaterializeApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*Materialize, error) {
	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient(), host.Builds)
	var store, err = newConnectorStore(recorder)
	if err != nil {
		return nil, fmt.Errorf("newConnectorStore: %w", err)
	}

	var out = &Materialize{
		coordinator: coordinator,
		host:        host,
		fsm:         nil,
		store:       store,
		taskReader:  taskReader{},
		taskTerm:    taskTerm{},
	}

	return out, nil
}

// RestoreCheckpoint establishes a driver connection and begins a Transactions RPC.
// It queries the driver to select from the latest local or driver-persisted checkpoint.
func (m *Materialize) RestoreCheckpoint(shard consumer.Shard) (cp pf.Checkpoint, err error) {

	// Gracefully stop a running FSM loop.
	if m.fsm != nil {
		close(m.fsm.shutdownCh)
		<-m.fsm.doneCh

		if m.fsm.loopErr != io.EOF {
			return pf.Checkpoint{}, fmt.Errorf("stopping connector: %w", err)
		}
		m.fsm = nil
	}

	if err = m.initTerm(shard, m.host); err != nil {
		return pf.Checkpoint{}, err
	}

	defer func() {
		if err == nil {
			m.Log(log.DebugLevel, log.Fields{
				"materialization": m.labels.TaskName,
				"shard":           m.shardSpec.Id,
				"build":           m.labels.Build,
				"checkpoint":      cp,
			}, "initialized processing term")
		} else {
			m.Log(log.ErrorLevel, log.Fields{
				"error": err.Error(),
			}, "failed to initialize processing term")
		}
	}()

	var spec *pf.MaterializationSpec
	if err = m.build.Extract(func(db *sql.DB) error {
		spec, err = catalog.LoadMaterialization(db, m.labels.TaskName)
		return err
	}); err != nil {
		return pf.Checkpoint{}, err
	} else if err = m.initReader(&m.taskTerm, shard, spec.TaskShuffles(), m.host); err != nil {
		return pf.Checkpoint{}, err
	}

	// Build a new matFSM, then connect and open the transactions RPC.
	if m.fsm, err = newMatFSM(shard.FQN(), spec, m.schemaIndex, m.LogPublisher); err != nil {
		return pf.Checkpoint{}, err
	} else if err = m.fsm.connect(shard.Context(), m.host.Config.Flow.Network, m.LogPublisher); err != nil {
		return pf.Checkpoint{}, err
	}
	opened, err := m.fsm.open(m.labels, m.store.driverCheckpoint())
	if err != nil {
		return pf.Checkpoint{}, err
	}

	// If the store provided a Flow checkpoint, prefer that over
	// the |checkpoint| recovered from the local recovery log store.
	if b := opened.FlowCheckpoint; len(b) != 0 {
		if err = cp.Unmarshal(b); err != nil {
			return pf.Checkpoint{}, fmt.Errorf("unmarshal Opened.FlowCheckpoint: %w", err)
		}
	} else {
		// Otherwise restore locally persisted checkpoint.
		if cp, err = m.store.restoreCheckpoint(shard); err != nil {
			return pf.Checkpoint{}, fmt.Errorf("store.RestoreCheckpoint: %w", err)
		}
	}

	// Re-acknowledge the recovered commit.
	if err = m.fsm.reAcknowledge(); err != nil {
		return pf.Checkpoint{}, err
	}
	go m.fsm.fsmLoop()

	return cp, nil
}

// StartCommit implements consumer.Store.StartCommit
func (m *Materialize) StartCommit(shard consumer.Shard, cp pf.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	m.Log(log.DebugLevel, log.Fields{
		"materialization": m.labels.TaskName,
		"shard":           m.shardSpec.Id,
		"build":           m.labels.Build,
		"checkpoint":      cp,
	}, "StartCommit")

	m.fsm.startCommitCh <- cp

	var drain matDrain
	select {
	case drain = <-m.fsm.drainCh:
	case <-m.fsm.doneCh:
		return client.FinishedOperation(m.fsm.loopErr)
	}

	m.store.updateDriverCheckpoint(
		drain.prepared.DriverCheckpointJson,
		drain.prepared.Rfc7396MergePatch)

	// Arrange for our store to commit to its recovery log upon a future DriverCommitted.
	var logCommittedOp = m.store.startCommit(shard, cp, consumer.OpFutures{
		m.fsm.driverCommittedOp: struct{}{}})

	drain.do(logCommittedOp)

	// Combiners are complete for this transaction. Drain each binding.
	for i, combiner := range drain.combiners {
		if err := drainBinding(
			drain.flighted[i],
			combiner,
			b.DeltaUpdates,
			drain.tx,
			drain.txStaged,
			i,
		); err != nil {
			return client.FinishedOperation(err)
		}
	}

	// Wait for any |waitFor| operations. In practice this is always empty.
	// It would contain pending journal writes, but materializations don't issue any.
	for op := range waitFor {
		if op.Err() != nil {
			return client.FinishedOperation(fmt.Errorf("dependency failed: %w", op.Err()))
		}
	}

	// Tell the driver to commit.
	if err := pm.WriteCommit(m.driverTx, &m.request); err != nil {
		return client.FinishedOperation(err)
	}

	var prior, next *matTxnShared // = m.shared, newMatTxnShared(prior.combiners)

	var doneOp = client.NewAsyncOperation()

	go func(storeOp client.OpFuture) (__out error) {
		defer func() { doneOp.Resolve(__out) }()

		// Await the recovery log commit.
		if err := storeOp.Err(); err != nil {
			return fmt.Errorf("recovery log commit: %w", err)
		}
		// Acknowledge the recovery log commit to the driver.
		m.driverTxMu.Lock()
		var err = pm.WriteAcknowledge(m.driverTx, &m.request)
		m.driverTxMu.Unlock()

		if err != nil {
			return err
		}

		// Await its Acknowledged in response.
		if err := next.acknowledgedOp.Err(); err != nil {
			return fmt.Errorf("reading Acknowledged: %w", err)
		}

		return nil
	}(commitOp)

	return doneOp
}

// drainBinding drains the a single materialization binding by sending Store
// requests for its reduced documents.
func drainBinding(
	flighted map[string]json.RawMessage,
	combiner *bindings.Combine,
	deltaUpdates bool,
	driverTx pm.Driver_TransactionsClient,
	request **pm.TransactionRequest,
	binding int,
) error {
	// Precondition: |flighted| contains the precise set of keys for this binding in this transaction.
	// See FinalizeTxn.
	var remaining = len(flighted)

	// Drain the combiner into materialization Store requests.
	if err := combiner.Drain(func(full bool, docRaw json.RawMessage, packedKey, packedValues []byte) error {
		// Inlined use of string(packedKey) clues compiler escape analysis to avoid allocation.
		if _, ok := flighted[string(packedKey)]; !ok {
			var key, _ = tuple.Unpack(packedKey)
			return fmt.Errorf(
				"driver implementation error: "+
					"loaded key %v (rawKey: %q) was not requested by Flow in this transaction (document %s)",
				key,
				string(packedKey),
				string(docRaw),
			)
		}

		// We're using |full|, an indicator of whether the document was a full
		// reduction or a partial combine, to track whether the document exists
		// in the store. This works because we only issue reduce-left when a
		// document was provided by Loaded or was retained from a previous
		// transaction's Store.

		if err := pm.StageStore(driverTx, request, binding,
			packedKey, packedValues, docRaw, full,
		); err != nil {
			return err
		}

		// We can retain a bounded number of documents from this transaction
		// as a performance optimization, so that they may be directly available
		// to the next transaction without issuing a Load.
		if deltaUpdates || remaining >= cachedDocumentBound {
			delete(flighted, string(packedKey)) // Don't retain.
		} else {
			// We cannot reference |rawDoc| beyond this callback, and must copy.
			// Fortunately, StageStore did just that, appending the document
			// to the staged request Arena, which we can reference here because
			// Arena bytes are write-once.
			var s = (*request).Store
			flighted[string(packedKey)] = s.Arena.Bytes(s.DocsJson[len(s.DocsJson)-1])
		}

		remaining--
		return nil

	}); err != nil {
		return fmt.Errorf("combine.Finish: %w", err)
	}

	// We should have seen 1:1 combined documents for each flighted key.
	if remaining != 0 {
		log.WithFields(log.Fields{
			"remaining": remaining,
			"flighted":  len(flighted),
		}).Panic("combiner drained, but expected documents remainder != 0")
	}

	return nil
}

func awaitCommitted(driverRx <-chan materialize.TransactionResponse, result *client.AsyncOperation) {
	if rx, err := materialize.Rx(driverRx, true); err != nil {
		result.Resolve(fmt.Errorf("reading Committed: %w", err))
	} else if rx.Committed == nil {
		result.Resolve(fmt.Errorf("expected Committed, got %#v", rx))
	} else {
		result.Resolve(nil)
	}
}

// Destroy implements consumer.Store.Destroy
func (m *Materialize) Destroy() {
	if m.driverTx != nil {
		_ = m.driverTx.CloseSend()
	}
	m.taskTerm.destroy()
	m.store.destroy()
}

// Implementing shuffle.Store for Materialize
var _ shuffle.Store = (*Materialize)(nil)

// Coordinator implements shuffle.Store.Coordinator
func (m *Materialize) Coordinator() *shuffle.Coordinator {
	return m.coordinator
}

// Implementing runtime.Application for Materialize
var _ Application = (*Materialize)(nil)

// BeginTxn implements Application.BeginTxn and is a no-op.
func (m *Materialize) BeginTxn(shard consumer.Shard) error {
	return nil
}

// pollLoaded selects and processes Loaded responses which can be read without blocking.
func (m *Materialize) pollLoaded() error {
	for {
		if resp, err := materialize.Rx(m.driverRx, false); err != nil {
			return fmt.Errorf("reading Loaded: %w", err)
		} else if resp == nil {
			return nil // Nothing to poll.
		} else if resp.Loaded == nil {
			return fmt.Errorf("expected Loaded, got %#v", resp)
		} else if err := m.reduceLoaded(resp.Loaded); err != nil {
			return err
		}
	}
}

// reduceLoaded reduces documents of the Loaded response into the matched combiner.
func (m *Materialize) reduceLoaded(loaded *pm.TransactionResponse_Loaded) error {
	var b = loaded.Binding
	if b >= uint32(len(m.materialization.Bindings)) {
		return fmt.Errorf("driver error (binding %d out of range)", b)
	}
	var combiner = m.combiners[b]

	// Feed documents into the combiner as reduce-left operations.
	for _, slice := range loaded.DocsJson {
		if err := combiner.ReduceLeft(loaded.Arena.Bytes(slice)); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
	}
	return nil
}

// ConsumeMessage implements Application.ConsumeMessage
func (m *Materialize) ConsumeMessage(shard consumer.Shard, envelope message.Envelope, pub *message.Publisher) error {
	select {
	case <-m.committed.Done():
		// Iff we've already read Committed from the last transaction,
		// do a non-blocking poll of ready Loaded responses.
		if err := m.pollLoaded(); err != nil {
			return fmt.Errorf("polling pending: %w", err)
		}
	default:
		// If a prior transaction hasn't committed, then an awaitCommitted() task
		// is still running and already selecting from |m.driverRx|.
	}

	var doc = envelope.Message.(pf.IndexedShuffleResponse)
	var packedKey = doc.Arena.Bytes(doc.PackedKey[doc.Index])
	var uuid = doc.GetUUID()

	if message.GetFlags(uuid) == message.Flag_ACK_TXN {
		return nil // We just ignore the ACK documents.
	}

	// Find *Shuffle with equal pointer.
	var binding = -1 // Panic if no *Shuffle is matched.
	var flighted map[string]json.RawMessage
	var combiner *bindings.Combine
	var deltaUpdates bool

	for i := range m.materialization.Bindings {
		if &m.materialization.Bindings[i].Shuffle == doc.Shuffle {
			binding = i
			flighted = m.flighted[i]
			combiner = m.combiners[i]
			deltaUpdates = m.materialization.Bindings[i].DeltaUpdates
		}
	}

	if doc, ok := flighted[string(packedKey)]; ok && doc == nil {
		// We've already seen this key within this transaction.
	} else if ok {
		// We retained this document from the last transaction.
		if deltaUpdates {
			panic("we shouldn't have retained if deltaUpdates")
		}
		if err := combiner.ReduceLeft(doc); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
		flighted[string(packedKey)] = nil // Clear old value & mark as visited.
	} else {
		// This is a novel key.
		if !deltaUpdates {
			if err := pm.StageLoad(m.driverTx, &m.request, binding, packedKey); err != nil {
				return err
			}
		}
		flighted[string(packedKey)] = nil // Mark as visited.
	}

	if err := combiner.CombineRight(doc.Arena.Bytes(doc.DocsJson[doc.Index])); err != nil {
		return fmt.Errorf("combiner.CombineRight: %w", err)
	}

	return nil
}

// FinalizeTxn implements Application.FinalizeTxn
func (m *Materialize) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	// Transactions may begin only after the last has committed.
	select {
	case <-m.committed.Done(): // Pass.
	default:
		panic("committed is not Done")
	}

	// Any remaining flighted keys *not* having `nil` values are retained documents
	// of a prior transaction which were not updated during this one.
	// We garbage collect them here, and achieve the StartCommit precondition that
	// |m.flighted| holds only keys of the current transaction with `nil` sentinels.
	for _, flighted := range m.flighted {
		for key, doc := range flighted {
			if doc != nil {
				delete(flighted, key)
			}
		}
	}

	log.WithFields(log.Fields{
		"shard":    shard.Spec().Id,
		"flighted": len(m.flighted),
	}).Trace("FinalizeTxn")

	return nil
}

// FinishedTxn implements Application.FinishedTxn
func (m *Materialize) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {
	logTxnFinished(m.LogPublisher, op)
}

// TODO(johnny): This is an interesting knob that should be exposed.
const cachedDocumentBound = 2048
