package main

import (
	"context"
	"encoding/json"
	"errors"
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
	c, err := consumer.NewKafkaConsumer(msgCH)

	if err != nil {
		panic(err)
	}

	return &Server{
		producer:  producer.NewKafkaProducer(""),
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
		s.producer.Producer(string(payload))
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
	repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		fmt.Printf("starting DB operation for OFFSET = %d EventID = %s\n", msg.Metadata.Offset, msg.Event.EventId)
		// TODO -> how to handle insert error
		defer s.consumer.MarkAsComplete(msg.Metadata)

		event := s.eventRepo.Get(ctx, tx, msg.Event.EventId)
		if event != nil {
			eMsg := fmt.Sprintf("offset = %d, eventID %s already existing -> skipping \n", msg.Metadata.Offset, msg.Event.EventId)
			return "", errors.New(eMsg)
		}

		id, err := s.eventRepo.Insert(ctx, tx, msg.Event)
		if err != nil {
			return "", err
		}

		fmt.Printf("INSERT SUCCESS, EventID = %s, Offset = %d\n", id, msg.Metadata.Offset)
		return id, nil
	})
}

func main() {
	db, err := repo.NewDBConn()
	if err != nil {
		panic(err)
	}
	er := repo.NewEventRepo(db)
	s := NewServer(er)

	go s.produceMsg()
	for msg := range s.msgCH {
		go s.handleMsg(msg)
	}

}
