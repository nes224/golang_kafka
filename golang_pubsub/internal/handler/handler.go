// Package handler — business logic ฝั่งรับ · พึ่งแค่ repo ไม่รู้จัก transport
package handler

import (
	"context"
	"log"

	"github.com/golang_pubsub/internal/repo"
	"github.com/golang_pubsub/internal/shared"
	"github.com/jmoiron/sqlx"
)

type Handler struct {
	eventRepo    *repo.EventRepo
	inboxRepo    *repo.InboxRepo
	consumerName string // = subscription id · ใช้เป็น "consumer" ใน inbox
}

func New(eventRepo *repo.EventRepo, inboxRepo *repo.InboxRepo, consumerName string) *Handler {
	return &Handler{eventRepo: eventRepo, inboxRepo: inboxRepo, consumerName: consumerName}
}

// Handle ตรงตาม signature ของ bus.Handler
//   - คืน nil → ack
//   - คืน err → nack (ส่งซ้ำ)
func (h *Handler) Handle(ctx context.Context, _ string, payload []byte) error {
	env, err := shared.DecodeEnvelope(payload)
	if err != nil {
		// poison message — decode ไม่ได้ retry ไปก็ไม่หาย → ack ทิ้ง (คืน nil)
		// production: ควรส่งไป Dead Letter Topic แทนการทิ้ง
		log.Printf("decode envelope: %v -> ACK (drop poison)", err)
		return nil
	}

	_, err = repo.TxClosure(ctx, h.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		// STEP 1 — dedup ก่อนแตะ business · ใน tx เดียวกัน (Inbox Pattern)
		isNew, err := h.inboxRepo.MarkProcessed(ctx, tx, env.EventID, h.consumerName)
		if err != nil {
			return "", err
		}
		if !isNew {
			log.Printf("SKIP duplicate event_id=%s", env.EventID)
			return "", nil // เคยทำแล้ว → ข้าม แต่ถือว่าสำเร็จ (ack)
		}

		// STEP 2 — event ใหม่ → ทำ business (insert ลง events) · atomic กับ inbox
		id, err := h.eventRepo.Insert(ctx, tx, &repo.Event{
			EventID:   env.EventID,
			EventType: env.EventType,
			Timestamp: env.Timestamp,
		})
		if err != nil {
			return "", err
		}
		log.Printf("INSERT ok event_id=%s", id)
		return id, nil
	})

	// err != nil → tx rollback (ทั้ง inbox + business) → คืน err → Pub/Sub จะ redeliver
	return err
}
