// Service A — producer · ยิง event อย่างเดียว (ไม่ต่อ DB)
// จำลอง service ฝั่งส่ง เช่น HR ที่ publish hr.projects
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/golang_pubsub/internal/producer"
	pubsubx "github.com/golang_pubsub/internal/pubsub"
	"github.com/golang_pubsub/internal/shared"
)

func main() {
	cfg := shared.NewConfig()

	// graceful shutdown (SIGINT/SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := pubsubx.NewClient(ctx, cfg)
	if err != nil {
		log.Fatalf("[producer] pubsub client: %v", err)
	}
	defer client.Close()

	// producer เป็นเจ้าของ topic → ensure ให้มี (idempotent · ขึ้นก่อน/หลัง consumer ก็ได้)
	topic, err := pubsubx.EnsureTopic(ctx, client, cfg.TopicID)
	if err != nil {
		log.Fatalf("[producer] ensure topic: %v", err)
	}

	pub := pubsubx.NewPublisher(topic, cfg.EnableOrdering)
	prod := producer.New(pub)

	log.Printf("[producer] publishing · project=%s topic=%s ordering=%v",
		cfg.ProjectID, cfg.TopicID, cfg.EnableOrdering)

	prod.Run(ctx) // บล็อกจน ctx ถูกยกเลิก

	if err := pub.Close(); err != nil { // flush ที่ค้าง
		log.Printf("[producer] publisher close: %v", err)
	}
	log.Println("[producer] shutdown complete")
}
