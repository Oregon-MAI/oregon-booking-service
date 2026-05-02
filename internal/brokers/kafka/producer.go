package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Producer struct {
	syncProducer sarama.SyncProducer
	log          *slog.Logger
	tracer       trace.Tracer
}

func NewProducer(brokers []string, clientID string, log *slog.Logger) (*Producer, error) {
	const op = "kafka.NewProducer"

	if log == nil {
		log = slog.Default()
	}

	if len(brokers) == 0 {
		return nil, fmt.Errorf("%s: no Kafka brokers provided", op)
	}

	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Partitioner = sarama.NewHashPartitioner

	syncProducer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: create sync producer: %w", op, err)
	}

	return &Producer{
		syncProducer: syncProducer,
		log:          log,
		tracer:       otel.GetTracerProvider().Tracer("kafka/producer"),
	}, nil
}

func (p *Producer) ProduceEvent(ctx context.Context, topic string, key string, msg any) error {
	const op = "kafka.PublishJSON"

	if p == nil || p.syncProducer == nil {
		return fmt.Errorf("%s: producer is not initialized", op)
	}

	ctx, span := p.tracer.Start(ctx, "Kafka.ProduceEvent")
	defer span.End()

	if topic == "" {
		return fmt.Errorf("%s: topic is required", op)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("%s: context canceled: %w", op, ctx.Err())
	default:
	}

	payload, err := marshalPayload(msg)
	if err != nil {
		return fmt.Errorf("%s: marshal message to JSON: %w", op, err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(payload),
	}
	if key != "" {
		message.Key = sarama.StringEncoder(key)
	}

	partition, offset, err := p.syncProducer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("%s: send message: %w", op, err)
	}

	p.log.DebugContext(ctx, "kafka event published",
		slog.String("topic", topic),
		slog.String("key", key),
		slog.Int("partition", int(partition)),
		slog.Int64("offset", offset),
	)

	return nil
}

func marshalPayload(msg any) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	switch v := msg.(type) {
	case []byte:
		return v, nil
	case json.RawMessage:
		return v, nil
	default:
		return json.Marshal(msg)
	}
}

func (p *Producer) Close() error {
	if p == nil || p.syncProducer == nil {
		return nil
	}

	if err := p.syncProducer.Close(); err != nil {
		return fmt.Errorf("kafka.Close: %w", err)
	}

	return nil
}
