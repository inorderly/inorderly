package broker

import (
	"context"
	"errors"
)

func kafkaEnd(ctx context.Context, e *Endpoint) (string, error) {
	return "", errors.New("kafka: not yet")
}
func openKafkaSource(ctx context.Context, e *Endpoint, from, until string) (Source, error) {
	return nil, errors.New("kafka: not yet")
}
func openKafkaSink(ctx context.Context, e *Endpoint) (Sink, error) {
	return nil, errors.New("kafka: not yet")
}

func kafkaProduce(ctx context.Context, e *Endpoint, values [][]byte) error {
	return errors.New("kafka: not yet")
}
