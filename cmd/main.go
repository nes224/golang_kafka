package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang_kafka/internal/consumer"
	"github.com/golang_kafka/internal/producer"
	"github.com/golang_kafka/internal/repo"
	"github.com/golang_kafka/internal/shared"
	"github.com/jmoiron/sqlx"
)

type Server struct {
	producer  *producer.KafkaProducer
	consumer  *consumer.KafkaConsumer
	msgCH     chan *shared.Message
	eventRepo *repo.EventRepo
}

func NewServer(eventRepo *repo.EventRepo) *Server {
	msgCH := make(chan *shared.Message, 64)
	c := consumer.NewKafkaConsumer(msgCH) // คืนค่าเดียว ไม่มี err แล้ว

	return &Server{
		producer:  producer.NewKafkaProducer(), // ไม่รับ arg แล้ว
		consumer:  c,
		msgCH:     msgCH,
		eventRepo: eventRepo,
	}
}

func (s *Server) produceMsg() {
	ticket := time.NewTicker(time.Second)

	defer ticket.Stop()

	for range ticket.C {
		// สร้าง Event แล้ว marshal เป็น JSON ให้ตรงกับที่ consumer (NewMessage) จะ unmarshal
		event := repo.NewEvent()
		payload, err := json.Marshal(event)
		if err != nil {
			fmt.Printf("error marshalling event = %v\n", err)
			continue
		}
		s.producer.Produce(payload) // Produce รับ []byte ตรงๆ (payload เป็น []byte อยู่แล้ว)
	}
}

func (s *Server) handleMsg(msg *shared.Message) {
	ctx := context.Background()
	s.saveToDB(ctx, msg)
}

// 1. do we need to get -> NO
// 2. do we lock db, higher isolation level -> No
// 3. ctx + tx -> YES
func (s *Server) saveToDB(ctx context.Context, msg *shared.Message) {
	_, err := repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		fmt.Printf("starting DB operation for OFFSET = %d EventID = %s\n", msg.Metadata.Offset, msg.Event.EventId)

		event := s.eventRepo.Get(ctx, tx, msg.Event.EventId)
		if event != nil {
			// เคย process แล้ว (idempotent) — ข้าม แต่ถือว่าสำเร็จ
			return "", nil
		}

		id, err := s.eventRepo.Insert(ctx, tx, msg.Event)
		if err != nil {
			return "", err
		}

		fmt.Printf("INSERT SUCCESS, EventID = %s, Offset = %d\n", id, msg.Metadata.Offset)
		return id, nil
	})

	// อัปเดต state ตามผลจริง → ให้ commit loop รู้ว่า offset นี้จบยังไง (Success/Error)
	if err != nil {
		s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Error)
		return
	}
	s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Success)
}

func main() {
	db, err := repo.NewDBConn()
	if err != nil {
		panic(err)
	}
	er := repo.NewEventRepo(db)
	s := NewServer(er)

	go s.consumer.RunConsumer() // เริ่ม consumeLoop + checkReadyToAccept (เดิม constructor ทำให้ ตอนนี้ต้องเรียกเอง)
	go s.produceMsg()

	for msg := range s.msgCH {
		go s.handleMsg(msg)
	}
}
