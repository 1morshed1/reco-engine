// Package events wraps Kafka production and consumption of user activity events.
// It hides the kafka-go client behind a tiny, JSON-based interface so the
// processor and producer deal in models.Event, not wire details (see docs/06).
package events

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/1morshed1/reco-engine/internal/models"
	"github.com/segmentio/kafka-go"
)

// Producer publishes events to the user-events topic.
type Producer struct {
	w *kafka.Writer
}

// NewProducer creates a Kafka writer. Events are keyed by user ID so all of one
// user's activity lands on the same partition and stays ordered — important
// because profile updates (recent list, running embedding) are order-sensitive.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // hash(key) -> partition, stable per user
			BatchTimeout: 50 * time.Millisecond,
			RequiredAcks: kafka.RequireOne,
			// Create the topic on first write if it doesn't exist yet (the
			// broker has auto-create enabled); avoids a manual topic setup step.
			AllowAutoTopicCreation: true,
		},
	}
}

// Publish serializes and sends one event.
func (p *Producer) Publish(ctx context.Context, ev models.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatInt(ev.UserID, 10)),
		Value: b,
	})
}

// Close flushes and closes the writer.
func (p *Producer) Close() error { return p.w.Close() }

// Consumer reads events from the topic as part of a consumer group, so multiple
// processor instances can share the partitions and offsets are tracked in Kafka.
type Consumer struct {
	r *kafka.Reader
}

// NewConsumer creates a group consumer. ReadMessage auto-commits offsets after a
// successful read, giving at-least-once delivery (see docs/06).
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
			// Start at the beginning the first time this group runs, so a fresh
			// stack replays existing history into Redis.
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10 << 20, // 10MB
		}),
	}
}

// Read blocks for the next event and decodes it.
func (c *Consumer) Read(ctx context.Context) (models.Event, error) {
	msg, err := c.r.ReadMessage(ctx)
	if err != nil {
		return models.Event{}, err
	}
	var ev models.Event
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return models.Event{}, err
	}
	return ev, nil
}

// Close closes the reader.
func (c *Consumer) Close() error { return c.r.Close() }
