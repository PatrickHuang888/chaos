package core

import (
	"context"
)

// RequestGenerator generates a request.
type RequestGenerator interface {
	Generate() interface{}
}

// Client applies the request to the database.
// Client is used in contorl.
// You should define your own client for your database.
type Client interface {
	Setup(ctx context.Context, node string, initData bool) error
	Close(ctx context.Context, nodes []string, node string) error
	Invoke(ctx context.Context, node string, r interface{}) interface{}
	NextRequest() interface{}
}

// ClientCreator creates a client.
// The control will create one client for one node.
type ClientCreator interface {
	// Create creates the client.
	Create(node string) Client
}

// NoopClientCreator creates a noop client.
type NoopClientCreator struct {
}

// Create creates the client.
func (NoopClientCreator) Create(node string) Client {
	return noopClient{}
}

// noopClient is a noop client
type noopClient struct {
}

// SetUp sets up the client.
func (noopClient) Setup(ctx context.Context, node string, initData bool) error { return nil }

// TearDown tears down the client.
func (noopClient) Close(ctx context.Context, nodes []string, node string) error { return nil }

// Invoke invokes a request to the database.
func (noopClient) Invoke(ctx context.Context, node string, r interface{}) interface{} {
	return 0
}

// NextRequest generates a request for latter Invoke.
func (noopClient) NextRequest() interface{} {
	return 1
}
