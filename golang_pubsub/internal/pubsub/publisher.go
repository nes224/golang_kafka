package pubsubx

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/golang_pubsub/internal/bus"
)

// Publisher — implement bus.Publisher ด้วย Pub/Sub topic
type Publisher struct {
	topic   *pubsub.Topic
	ordered bool
}

// ยืนยันตอน compile ว่า *Publisher ทำตาม interface bus.Publisher ครบ
var _ bus.Publisher = (*Publisher)(nil)

func NewPublisher(topic *pubsub.Topic, ordered bool) *Publisher {
	if ordered {
		// ต้องเปิด flag นี้ฝั่ง topic ด้วยถึงจะใช้ OrderingKey ได้
		topic.EnableMessageOrdering = true
	}
	return &Publisher{topic: topic, ordered: ordered}
}

func (p *Publisher) Publish(ctx context.Context, key string, payload []byte) error {
	msg := &pubsub.Message{Data: payload}
	if p.ordered {
		msg.OrderingKey = key // event key เดียวกันจะถูกส่งตามลำดับ (แทน partition ของ Kafka)
	}

	res := p.topic.Publish(ctx, msg)

	// res.Get บล็อกรอ server ยืนยันว่ารับแล้ว → ได้ durability guarantee
	// (เทียบ acks=all ของ Kafka producer) · ถ้า fail คืน error ให้ caller จัดการ
	_, err := res.Get(ctx)
	return err
}

// Close — flush message ที่ค้างใน buffer ก่อนปิด (graceful)
// ไม่ปิด *pubsub.Client เพราะ client ถูกแชร์กับ consumer — ปล่อยให้ main ปิดทีเดียว
func (p *Publisher) Close() error {
	p.topic.Stop()
	return nil
}
