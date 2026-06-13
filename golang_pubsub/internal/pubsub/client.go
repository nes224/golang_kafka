// Package pubsubx — implementation ของ bus.Publisher / bus.Consumer ด้วย GCP Pub/Sub
// (ตั้งชื่อ package เป็น pubsubx เพื่อไม่ชนกับ import "cloud.google.com/go/pubsub")
package pubsubx

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/golang_pubsub/internal/shared"
)

// NewClient สร้าง Pub/Sub client
// ถ้า env PUBSUB_EMULATOR_HOST ถูกตั้งไว้ library จะต่อ emulator ให้อัตโนมัติ
// (ไม่ต้องมี GCP credential) — เหมาะกับการรัน local
func NewClient(ctx context.Context, cfg *shared.Config) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, cfg.ProjectID)
}

// EnsureTopic — สร้าง topic ถ้ายังไม่มี (เทียบ initializeKafkaTopic ของฝั่ง Kafka)
func EnsureTopic(ctx context.Context, c *pubsub.Client, topicID string) (*pubsub.Topic, error) {
	t := c.Topic(topicID)
	ok, err := t.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("check topic %q: %w", topicID, err)
	}
	if !ok {
		if _, err := c.CreateTopic(ctx, topicID); err != nil {
			return nil, fmt.Errorf("create topic %q: %w", topicID, err)
		}
	}
	return c.Topic(topicID), nil
}

// EnsureSubscription — สร้าง subscription (= consumer group) ผูกกับ topic ถ้ายังไม่มี
//
// AckDeadline = เวลาที่ Pub/Sub รอ ack ก่อนถือว่า fail แล้วส่งซ้ำ
//   (เทียบได้กับ "ถ้า process ไม่จบใน X วิ → redeliver") — คล้ายแนวคิด max.poll.interval ของ Kafka
func EnsureSubscription(
	ctx context.Context,
	c *pubsub.Client,
	subID string,
	topic *pubsub.Topic,
	ackDeadline time.Duration,
	ordered bool,
) (*pubsub.Subscription, error) {
	s := c.Subscription(subID)
	ok, err := s.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("check subscription %q: %w", subID, err)
	}
	if !ok {
		_, err := c.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
			Topic:                 topic,
			AckDeadline:           ackDeadline,
			EnableMessageOrdering: ordered,
		})
		if err != nil {
			return nil, fmt.Errorf("create subscription %q: %w", subID, err)
		}
	}
	return c.Subscription(subID), nil
}
