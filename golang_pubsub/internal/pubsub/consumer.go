package pubsubx

import (
	"context"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/golang_pubsub/internal/bus"
)

// Consumer — implement bus.Consumer ด้วย Pub/Sub subscription
//
// ★ จุดต่างใหญ่จาก Kafka:
//   - ไม่มี offset / ไม่มี manual commit / ไม่มี sequential-commit loop
//   - ack/nack ทีละ message โดยตรง → กลไก PartitionState + commitOffsetLoop หายไปทั้งก้อน
//   - ไม่มี rebalance callback (assign/revoke) → Pub/Sub โหลดบาลานซ์ให้ server-side
//   - scale: รันหลาย process บน subscription เดียวกัน Pub/Sub แบ่ง message ให้เอง
type Consumer struct {
	sub *pubsub.Subscription
}

var _ bus.Consumer = (*Consumer)(nil)

func NewConsumer(sub *pubsub.Subscription, maxOutstanding int) *Consumer {
	// ReceiveSettings คุม concurrency — เทียบได้กับการ fan-out "go handleMsg" ฝั่ง Kafka
	// MaxOutstandingMessages = จำนวน message ที่ค้าง process ได้พร้อมกัน (flow control)
	sub.ReceiveSettings.MaxOutstandingMessages = maxOutstanding
	return &Consumer{sub: sub}
}

func (c *Consumer) Receive(ctx context.Context, handler bus.Handler) error {
	// Receive เรียก callback แบบขนานหลาย goroutine ให้อัตโนมัติ · บล็อกจน ctx ถูกยกเลิก
	return c.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		if err := handler(ctx, m.OrderingKey, m.Data); err != nil {
			log.Printf("handler error msgID=%s: %v -> NACK (จะส่งซ้ำ)", m.ID, err)
			m.Nack() // ส่งซ้ำหลัง ack deadline — รับประกัน at-least-once
			return
		}
		m.Ack() // สำเร็จ → ack ทันที (ไม่ต้องรอ commit loop เหมือน Kafka)
	})
}

// Close — Receive จะหยุดเองเมื่อ ctx ถูกยกเลิก จึงไม่มีอะไรต้องทำเพิ่ม
func (c *Consumer) Close() error { return nil }
