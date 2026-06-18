// Service B — consumer · รับ event ไป process ลง DB (ไม่ publish)
// จำลอง service ฝั่งรับ เช่น Warehouse ที่ consume hr.projects ไป upsert
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang_pubsub/internal/handler"
	pubsubx "github.com/golang_pubsub/internal/pubsub"
	"github.com/golang_pubsub/internal/repo"
	"github.com/golang_pubsub/internal/shared"
)

func main() {
	cfg := shared.NewConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---------- DB (ฝั่ง consumer เท่านั้นที่ต่อ DB) ----------
	db, err := repo.NewDBConn()
	if err != nil {
		log.Fatalf("[consumer] db connect: %v", err)
	}
	defer db.Close()
	eventRepo := repo.NewEventRepo(db)
	inboxRepo := repo.NewInboxRepo()

	// ---------- Pub/Sub ----------
	client, err := pubsubx.NewClient(ctx, cfg)
	if err != nil {
		log.Fatalf("[consumer] pubsub client: %v", err)
	}
	defer client.Close()

	// ensure topic ด้วย (เผื่อ consumer ขึ้นก่อน producer) + ensure subscription
	topic, err := pubsubx.EnsureTopic(ctx, client, cfg.TopicID)
	if err != nil {
		log.Fatalf("[consumer] ensure topic: %v", err)
	}
	sub, err := pubsubx.EnsureSubscription(ctx, client, cfg.SubscriptionID, topic, 30*time.Second, cfg.EnableOrdering)
	if err != nil {
		log.Fatalf("[consumer] ensure subscription: %v", err)
	}

	con := pubsubx.NewConsumer(sub, 64)
	h := handler.New(eventRepo, inboxRepo, cfg.SubscriptionID)

	log.Printf("[consumer] listening · subscription=%s topic=%s", cfg.SubscriptionID, cfg.TopicID)

	// Receive บล็อกจน ctx ถูกยกเลิก (Ctrl+C)
	if err := con.Receive(ctx, h.Handle); err != nil && ctx.Err() == nil {
		log.Fatalf("[consumer] receive: %v", err)
	}
	log.Println("[consumer] shutdown complete")
}
