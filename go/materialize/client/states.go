package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	"go.gazette.dev/core/broker/client"
)

type state int

const (
	txLoadCommit      state = iota
	txLoadAcknowledge state = iota
	txPrepare         state = iota
	rxPrepared        state = iota
	rxPendingCommit   state = iota
	rxDriverCommitted state = iota
	rxAcknowledged    state = iota
)

type Client struct {
	spec *pf.MaterializationSpec // Read-only.

	tx struct {
		client      pm.Driver_TransactionsClient
		commitOpsCh chan<- CommitOps
		preparedCh  <-chan pm.TransactionResponse_Prepared
		staged      *pm.TransactionRequest
		state       state
		sync.Mutex
	}
	rx struct {
		commitOps   CommitOps
		commitOpsCh <-chan CommitOps
		loopOp      *client.AsyncOperation
		preparedCh  chan<- pm.TransactionResponse_Prepared
		respCh      <-chan materialize.TransactionResponse
		state       state
	}
	shared struct {
		combiners []*bindings.Combine
		flighted  []map[string]json.RawMessage
		sync.Mutex
	}
}

func NewClient(
	ctx context.Context,
	driverCheckpoint json.RawMessage,
	fqn string,
	labeling labels.ShardLabeling,
	logger ops.Logger,
	network string,
	schemaIndex *bindings.SchemaIndex,
	spec *pf.MaterializationSpec,
) (*Client, error) {

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

	// Establish driver connection and start Transactions RPC.
	conn, err := materialize.NewDriver(ctx,
		spec.EndpointType,
		spec.EndpointSpecJson,
		network,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("building endpoint driver: %w", err)
	}
	rpc, err := conn.Transactions(ctx)
	if err != nil {
		return nil, fmt.Errorf("driver.Transactions: %w", err)
	}

	var preparedCh = make(chan pm.TransactionResponse_Prepared)
	var commitOpsCh = make(chan CommitOps)

	var out = &Client{spec: spec}
	out.tx.client = rpc
	out.tx.commitOpsCh = commitOpsCh
	out.tx.preparedCh = preparedCh
	out.tx.staged = nil
	out.tx.state = txLoadAcknowledge

	out.rx.commitOps = CommitOps{}
	out.rx.commitOpsCh = commitOpsCh
	out.rx.loopOp = client.NewAsyncOperation()
	out.rx.preparedCh = preparedCh
	out.rx.respCh = materialize.TransactionResponseChannel(rpc)
	out.rx.state = rxAcknowledged

	out.shared.combiners = combiners
	out.shared.flighted = flighted

	return out, nil
}

func (f *Client) Close() error {
	f.tx.Lock()
	defer f.tx.Unlock()

	f.tx.client.CloseSend()
	<-f.rx.loopOp.Done()

	for _, c := range f.shared.combiners {
		c.Destroy()
	}
	return f.rx.loopOp.Err()
}

type CommitOps struct {
	DriverCommitted *client.AsyncOperation
	LogCommitted    client.OpFuture
	Acknowledged    *client.AsyncOperation
}

func (f *Client) AddDocument(binding int, packedKey []byte, doc json.RawMessage) error {
	f.tx.Lock()
	defer f.tx.Unlock()

	switch f.tx.state {
	case txLoadCommit, txLoadAcknowledge:
	default:
		return fmt.Errorf("caller protocol error: AddDocument is invalid in state %t", f.tx.state)
	}

	// Note that combineRight obtains a lock on |f.shared|, but it's not held
	// while we StageLoad to the connector (which could block).
	// This allows for a concurrent handling of a Loaded response.

	if load, err := f.combineRight(binding, packedKey, doc); err != nil {
		return err
	} else if !load {
		// No-op.
	} else if err = pm.StageLoad(f.tx.client, &f.tx.staged, binding, packedKey); err != nil {
		return err
	}

	// f.tx.state is unchanged.
	return nil
}

func (f *Client) Prepare(flowCheckpoint pf.Checkpoint) (pm.TransactionResponse_Prepared, error) {
	f.tx.Lock()
	defer f.tx.Unlock()

	if f.tx.state != txLoadAcknowledge {
		return pm.TransactionResponse_Prepared{}, fmt.Errorf(
			"client protocol error: SendPrepare is invalid in state %t", f.tx.state)
	}

	if err := pm.WritePrepare(f.tx.client, &f.tx.staged, flowCheckpoint); err != nil {
		return pm.TransactionResponse_Prepared{}, err
	}
	f.tx.state = txPrepare

	// We deliberately hold the |f.tx| lock while awaiting Prepared,
	// because the Prepare => Prepared interaction is synchronous.

	select {
	case prepared := <-f.tx.preparedCh:
		return prepared, nil
	case <-f.rx.loopOp.Done():
		return pm.TransactionResponse_Prepared{}, f.rx.loopOp.Err()
	}
}

func (f *Client) StartCommit(ops CommitOps) error {
	f.tx.Lock()
	defer f.tx.Unlock()

	if f.tx.state != txPrepare {
		return fmt.Errorf(
			"client protocol error: StartCommit is invalid in state %t", f.tx.state)
	}

	f.shared.Lock()
	defer f.shared.Unlock()

	// We hold both the |f.tx| and |f.shared| locks during the store phase.
	// The read loop accesses |f.shared| to handled Loaded responses,
	// but those are disallowed at this stage of the protocol.

	// Inform read loop of new CommitOps.
	select {
	case f.tx.commitOpsCh <- ops:
	case <-f.rx.loopOp.Done():
		return f.rx.loopOp.Err()
	}

	// Drain each binding.
	for i, combiner := range f.shared.combiners {
		if err := drainBinding(
			f.shared.flighted[i],
			combiner,
			f.spec.Bindings[i].DeltaUpdates,
			f.tx.client,
			&f.tx.staged,
			i,
		); err != nil {
			return err
		}
	}

	// Tell the driver to commit.
	if err := pm.WriteCommit(f.tx.client, &f.tx.staged); err != nil {
		return err
	}

	f.tx.state = txLoadCommit
	return nil
}

func (f *Client) onLogCommitted() error {
	if f.tx.TryLock() {
		defer f.tx.Unlock() // Common case.

		if err := pm.WriteAcknowledge(f.tx.client, &f.tx.staged); err != nil {
			return err
		}
		f.tx.state = txLoadAcknowledge
		return nil
	}

	go func() {
		f.tx.Lock()
		defer f.tx.Unlock()

		// An error indicates a broken connection, which we'll read from the receiver.
		_ = pm.WriteAcknowledge(f.tx.client, &f.tx.staged)
		f.tx.state = txLoadAcknowledge
	}()

	return nil
}

func (f *Client) readLoop() (__out error) {
	if f.rx.state != rxAcknowledged {
		panic(f.rx.state)
	}

	defer func() {
		f.rx.loopOp.Resolve(__out)

		// These can never succeed, since we're no longer looping.
		if f.rx.commitOps.DriverCommitted != nil {
			f.rx.commitOps.DriverCommitted.Resolve(__out)
		}
		if f.rx.commitOps.Acknowledged != nil {
			f.rx.commitOps.Acknowledged.Resolve(__out)
		}
	}()

	for {
		var maybeLogCommitted <-chan struct{}
		if f.rx.commitOps.LogCommitted != nil {
			maybeLogCommitted = f.rx.commitOps.LogCommitted.Done()
		}

		select {
		case <-maybeLogCommitted:
			if err := f.rx.commitOps.LogCommitted.Err(); err != nil {
				return fmt.Errorf("recovery log commit: %w", err)
			} else if err = f.onLogCommitted(); err != nil {
				return fmt.Errorf("onLogCommitted: %w", err)
			}

		case rx, ok := <-f.rx.respCh:
			if !ok {
				return io.EOF
			} else if rx.Error != nil {
				return rx.Error
			} else if err := rx.Validate(); err != nil {
				return err
			}

			switch {
			case rx.DriverCommitted != nil:
				if err := f.onDriverCommitted(*rx.DriverCommitted); err != nil {
					return fmt.Errorf("onDriverCommitted: %w", err)
				}
			case rx.Loaded != nil:
				if err := f.onLoaded(*rx.Loaded); err != nil {
					return fmt.Errorf("onLoaded: %w", err)
				}
			case rx.Acknowledged != nil:
				if err := f.onAcknowledged(*rx.Acknowledged); err != nil {
					return fmt.Errorf("onAcknowledged: %w", err)
				}
			case rx.Prepared != nil:
				if err := f.onPrepared(*rx.Prepared); err != nil {
					return fmt.Errorf("onPrepared: %w", err)
				}
			default:
				panic(fmt.Sprint(rx))
			}
		}
	}
	return nil
}

func (f *Client) onDriverCommitted(pm.TransactionResponse_DriverCommitted) error {
	if f.rx.state != rxPendingCommit {
		return fmt.Errorf("connector protocol error (DriverCommitted not expected in state %v)", f.rx.state)
	}

	// This future was used as a recovery log write dependency by StartCommit of
	// the last transaction. Resolving it allows that recovery log write to proceed,
	// which we'll observe as a future resolution of fsm.logCommittedOp.
	f.rx.commitOps.DriverCommitted.Resolve(nil)
	f.rx.commitOps.DriverCommitted = nil

	f.rx.state = rxDriverCommitted
	return nil
}

func (f *Client) onLoaded(loaded pm.TransactionResponse_Loaded) error {
	switch f.rx.state {
	case rxDriverCommitted, rxAcknowledged:
		// Pass.
	default:
		return fmt.Errorf("connector protocol error (Loaded not expected in state %v)", f.rx.state)
	}

	f.shared.Lock()
	defer f.shared.Unlock()

	if int(loaded.Binding) > len(f.shared.combiners) {
		return fmt.Errorf("driver error (binding %d out of range)", loaded.Binding)
	}

	// Feed documents into the combiner as reduce-left operations.
	var combiner = f.shared.combiners[loaded.Binding]
	for _, slice := range loaded.DocsJson {
		if err := combiner.ReduceLeft(loaded.Arena.Bytes(slice)); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
	}

	// f.rx.state is unchanged.
	return nil
}

func (f *Client) onAcknowledged(pm.TransactionResponse_Acknowledged) error {
	if f.rx.state != rxDriverCommitted {
		return fmt.Errorf("connector protocol error (Acknowledged not expected in state %v)", f.rx.state)
	}

	// This future was returned by StartCommit of the last transaction.
	// Gazette holds the current transaction open until it resolves.
	f.rx.commitOps.Acknowledged.Resolve(nil)
	f.rx.commitOps.Acknowledged = nil

	f.rx.state = rxAcknowledged
	return nil
}

func (f *Client) onPrepared(prepared pm.TransactionResponse_Prepared) error {
	if f.rx.state != rxAcknowledged {
		return fmt.Errorf("connector protocol error (Prepared not expected in state %v)", f.rx.state)
	}

	// Tell synchronous Client.Prepare() of this response.
	f.rx.preparedCh <- prepared

	f.rx.state = rxPrepared
	return nil
}

func (f *Client) onCommitOps(ops CommitOps) error {
	if f.rx.state != rxPrepared {
		return fmt.Errorf("client protocol error (StartCommit not expected in state %v)", f.rx.state)
	}

	f.rx.commitOps = ops

	f.rx.state = rxPendingCommit
	return nil
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

func (f *Client) combineRight(binding int, packedKey []byte, doc json.RawMessage) (bool, error) {
	f.shared.Lock()
	defer f.shared.Unlock()

	var flighted = f.shared.flighted[binding]
	var combiner = f.shared.combiners[binding]
	var deltaUpdates = f.spec.Bindings[binding].DeltaUpdates
	var load bool

	if doc, ok := flighted[string(packedKey)]; ok && doc == nil {
		// We've already seen this key within this transaction.
	} else if ok {
		// We retained this document from the last transaction.
		if deltaUpdates {
			panic("we shouldn't have retained if deltaUpdates")
		}
		if err := combiner.ReduceLeft(doc); err != nil {
			return false, fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
		flighted[string(packedKey)] = nil // Clear old value & mark as visited.
	} else {
		// This is a novel key.
		load = !deltaUpdates
		flighted[string(packedKey)] = nil // Mark as visited.
	}

	if err := combiner.CombineRight(doc); err != nil {
		return false, fmt.Errorf("combiner.CombineRight: %w", err)
	}

	return load, nil
}
