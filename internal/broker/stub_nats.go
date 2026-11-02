package broker

import (
	"context"
	"errors"
)

func natsEnd(ctx context.Context, e *Endpoint) (string, error) {
	return "", errors.New("nats: not yet")
}
func openNATSSource(ctx context.Context, e *Endpoint, from, until string) (Source, error) {
	return nil, errors.New("nats: not yet")
}
func openNATSSink(ctx context.Context, e *Endpoint) (Sink, error) {
	return nil, errors.New("nats: not yet")
}

func natsProduce(ctx context.Context, e *Endpoint, values [][]byte) error {
	return errors.New("nats: not yet")
}
