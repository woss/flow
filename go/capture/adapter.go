package capture

// TODO(johnny): While they started a bit different, over time this file and
// go/materialize/adapter.go now look essentially identical in terms of
// structure. They do differ on interfaces, however, making re-use a challenge.
// If contemplating a change here, make it there as well.
// And when generally available, consider using Go generics ?

import (
	"context"
	"io"

	pc "github.com/estuary/protocols/capture"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// pullRequest is a channel-oriented wrapper of pc.PullRequest
type pullRequest struct {
	*pc.PullRequest
	Error error
}

// PullResponse is a channel-oriented wrapper of pc.PullResponse.
type PullResponse struct {
	*pc.PullResponse
	Error error
}

// PullResponseChannel spawns a goroutine which receives
// from the stream and sends responses into the returned channel,
// which is closed after the first encountered read error.
// As an optimization, it avoids this read loop if the stream
// is an in-process adapter.
func PullResponseChannel(stream pc.Driver_PullClient) <-chan PullResponse {
	if adapter, ok := stream.(*adapterStreamClient); ok {
		return adapter.rx
	}

	var ch = make(chan PullResponse, 4)
	go func() {
		for {
			// Use Recv because ownership of |m| is transferred to |ch|,
			// and |m| cannot be reused.
			var m, err = stream.Recv()

			if err == nil {
				ch <- PullResponse{PullResponse: m}
				continue
			}

			if err != io.EOF {
				ch <- PullResponse{Error: err}
			}
			close(ch)
			return
		}
	}()

	return ch
}

// Rx receives from a PullResponse channel.
// It destructures PullResponse into its parts,
// and also returns an explicit io.EOF for channel closures.
func Rx(ch <-chan PullResponse, block bool) (*pc.PullResponse, error) {
	var rx PullResponse
	var ok bool

	if block {
		rx, ok = <-ch
	} else {
		select {
		case rx, ok = <-ch:
		default:
			ok = true
		}
	}

	if !ok {
		return nil, io.EOF
	} else if rx.Error != nil {
		return nil, rx.Error
	} else {
		return rx.PullResponse, nil
	}
}

// AdaptServerToClient wraps an in-process DriverServer to provide a DriverClient.
func AdaptServerToClient(srv pc.DriverServer) pc.DriverClient {
	return adapter{srv}
}

// adapter is pc.DriverClient that wraps an in-process pc.DriverServer.
type adapter struct{ pc.DriverServer }

func (a adapter) Spec(ctx context.Context, in *pc.SpecRequest, opts ...grpc.CallOption) (*pc.SpecResponse, error) {
	return a.DriverServer.Spec(ctx, in)
}

func (a adapter) Discover(ctx context.Context, in *pc.DiscoverRequest, opts ...grpc.CallOption) (*pc.DiscoverResponse, error) {
	return a.DriverServer.Discover(ctx, in)
}

func (a adapter) Validate(ctx context.Context, in *pc.ValidateRequest, opts ...grpc.CallOption) (*pc.ValidateResponse, error) {
	return a.DriverServer.Validate(ctx, in)
}

func (a adapter) ApplyUpsert(ctx context.Context, in *pc.ApplyRequest, opts ...grpc.CallOption) (*pc.ApplyResponse, error) {
	return a.DriverServer.ApplyUpsert(ctx, in)
}

func (a adapter) ApplyDelete(ctx context.Context, in *pc.ApplyRequest, opts ...grpc.CallOption) (*pc.ApplyResponse, error) {
	return a.DriverServer.ApplyDelete(ctx, in)
}

func (a adapter) Pull(ctx context.Context, opts ...grpc.CallOption) (pc.Driver_PullClient, error) {
	var reqCh = make(chan pullRequest, 4)
	var respCh = make(chan PullResponse, 4)
	var doneCh = make(chan struct{})

	var clientStream = &adapterStreamClient{
		ctx:  ctx,
		tx:   reqCh,
		rx:   respCh,
		done: doneCh,
	}
	var serverStream = &adapterStreamServer{
		ctx: ctx,
		tx:  respCh,
		rx:  reqCh,
	}

	go func() (err error) {
		defer func() {
			if err != nil {
				respCh <- PullResponse{Error: err}
			}
			close(respCh)
			close(doneCh)
		}()
		return a.DriverServer.Pull(serverStream)
	}()

	return clientStream, nil
}

type adapterStreamClient struct {
	ctx  context.Context
	tx   chan<- pullRequest
	rx   <-chan PullResponse
	done <-chan struct{}
}

func (a *adapterStreamClient) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamClient) Send(m *pc.PullRequest) error {
	select {
	case a.tx <- pullRequest{PullRequest: m}:
		return nil
	case <-a.done:
		// The server already closed the RPC, revoking our ability to transmit.
		// Match gRPC behavior of returning io.EOF on Send, and the real error on Recv.
		return io.EOF
	}
}

func (a *adapterStreamClient) CloseSend() error {
	close(a.tx)
	return nil
}

func (a *adapterStreamClient) Recv() (*pc.PullResponse, error) {
	if m, ok := <-a.rx; ok {
		return m.PullResponse, m.Error
	}
	return nil, io.EOF
}

// Remaining panic implementations of grpc.ClientStream follow:

func (a *adapterStreamClient) Header() (metadata.MD, error) { panic("not implemented") }
func (a *adapterStreamClient) Trailer() metadata.MD         { panic("not implemented") }
func (a *adapterStreamClient) SendMsg(m interface{}) error  { panic("not implemented") } // Use Send.
func (a *adapterStreamClient) RecvMsg(m interface{}) error  { panic("not implemented") }

type adapterStreamServer struct {
	ctx context.Context
	tx  chan<- PullResponse
	rx  <-chan pullRequest
}

var _ pc.Driver_PullServer = new(adapterStreamServer)

func (a *adapterStreamServer) Context() context.Context {
	return a.ctx
}

func (a *adapterStreamServer) Send(m *pc.PullResponse) error {
	// Under the gRPC model, the server controls RPC termination. The client cannot
	// revoke the server's ability to send (in the absence of a broken transport,
	// which we don't model here).
	a.tx <- PullResponse{PullResponse: m}
	return nil
}

func (a *adapterStreamServer) Recv() (*pc.PullRequest, error) {
	if m, ok := <-a.rx; ok {
		return m.PullRequest, m.Error
	}
	return nil, io.EOF
}

func (a *adapterStreamServer) RecvMsg(m interface{}) error {
	if mm, ok := <-a.rx; ok && mm.Error == nil {
		*m.(*pc.PullRequest) = *mm.PullRequest
		return nil
	} else if ok {
		return mm.Error
	}
	return io.EOF
}

// Remaining panic implementations of grpc.ServerStream follow:

func (a *adapterStreamServer) SetHeader(metadata.MD) error  { panic("not implemented") }
func (a *adapterStreamServer) SendHeader(metadata.MD) error { panic("not implemented") }
func (a *adapterStreamServer) SetTrailer(metadata.MD)       { panic("not implemented") }
func (a *adapterStreamServer) SendMsg(m interface{}) error  { panic("not implemented") } // Use Send().
