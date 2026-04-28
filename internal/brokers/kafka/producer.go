package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

type Producer struct {
	syncProducer sarama.SyncProducer
	log          *slog.Logger
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

	return &Producer{syncProducer: syncProducer, log: log}, nil
}

func (p *Producer) ProduceEvent(ctx context.Context, topic string, key string, msg any) error {
	const op = "kafka.PublishJSON"

	if p == nil || p.syncProducer == nil {
		return fmt.Errorf("%s: producer is not initialized", op)
	}
	if topic == "" {
		return fmt.Errorf("%s: topic is required", op)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("%s: context canceled: %w", op, ctx.Err())
	default:
	}

	payload, err := json.Marshal(msg)
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

func (p *Producer) Close() error {
	if p == nil || p.syncProducer == nil {
		return nil
	}

	if err := p.syncProducer.Close(); err != nil {
		return fmt.Errorf("kafka.Close: %w", err)
	}

	return nil
}
