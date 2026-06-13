package producer

import (
	"context"
	"log"
	"time"

	"github.com/golang_pubsub/internal/bus"
	"github.com/golang_pubsub/internal/shared"
)

// Producer ยิง event ทุกวินาที (เหมือน produceMsg ของฝั่ง Kafka)
// พึ่งแค่ bus.Publisher → ไม่รู้จัก Pub/Sub ตรงๆ
type Producer struct {
	pub bus.Publisher
}

func New(pub bus.Publisher) *Producer { return &Producer{pub: pub} }

func (p *Producer) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("producer stopped")
			return
		case <-ticker.C:
			env := shared.NewEvent()
			payload, err := env.Encode()
			if err != nil {
				log.Printf("encode event: %v", err)
				continue
			}
			// key = EventType เพื่อ demo ordering (event ชนิดเดียวกันรักษาลำดับ ถ้าเปิด ordering)
			if err := p.pub.Publish(ctx, env.EventType, payload); err != nil {
				log.Printf("publish event_id=%s: %v", env.EventID, err)
				continue
			}
			log.Printf("PUBLISHED event_id=%s", env.EventID)
		}
	}
}
