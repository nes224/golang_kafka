package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang_pubsub/internal/handler"
	"github.com/golang_pubsub/internal/producer"
	pubsubx "github.com/golang_pubsub/internal/pubsub"
	"github.com/golang_pubsub/internal/repo"
	"github.com/golang_pubsub/internal/shared"
)

func main() {
	cfg := shared.NewConfig()

	// ctx ถูกยกเลิกเมื่อได้ SIGINT/SIGTERM → graceful shutdown
	// (แก้ Known Issue ของฝั่ง Kafka ที่ไม่มี graceful shutdown)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---------- DB ----------
	db, err := repo.NewDBConn()
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer db.Close()

	eventRepo := repo.NewEventRepo(db)
	inboxRepo := repo.NewInboxRepo()

	// ---------- Pub/Sub: client + topic + subscription ----------
	client, err := pubsubx.NewClient(ctx, cfg)
	if err != nil {
		log.Fatalf("pubsub client: %v", err)
	}
	defer client.Close() // main เป็นเจ้าของ lifecycle ของ client (publisher/consumer แชร์กัน)

	topic, err := pubsubx.EnsureTopic(ctx, client, cfg.TopicID)
	if err != nil {
		log.Fatalf("ensure topic: %v", err)
	}
	sub, err := pubsubx.EnsureSubscription(ctx, client, cfg.SubscriptionID, topic, 30*time.Second, cfg.EnableOrdering)
	if err != nil {
		log.Fatalf("ensure subscription: %v", err)
	}

	// ---------- ประกอบ (wire) ----------
	pub := pubsubx.NewPublisher(topic, cfg.EnableOrdering)
	con := pubsubx.NewConsumer(sub, 64)
	h := handler.New(eventRepo, inboxRepo, cfg.SubscriptionID)
	prod := producer.New(pub)

	// ---------- run ----------
	go prod.Run(ctx)

	log.Printf("listening · project=%s topic=%s subscription=%s ordering=%v",
		cfg.ProjectID, cfg.TopicID, cfg.SubscriptionID, cfg.EnableOrdering)

	// Receive บล็อกจน ctx ถูกยกเลิก (Ctrl+C)
	if err := con.Receive(ctx, h.Handle); err != nil && ctx.Err() == nil {
		log.Fatalf("receive: %v", err)
	}

	// graceful: flush publisher ที่ค้าง
	if err := pub.Close(); err != nil {
		log.Printf("publisher close: %v", err)
	}
	log.Println("shutdown complete")
}
