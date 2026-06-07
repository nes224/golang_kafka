package main

import (
	"fmt"
	"time"

	"github.com/golang_kafka/internal/consumer"
	"github.com/golang_kafka/internal/producer"
)

type Server struct {
	producer *producer.KafkaProducer
	consumer *consumer.KafkaConsumer
	msgCH    chan string
}

func NewServer() *Server {
	msgCH := make(chan string, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)

	if err != nil {
		panic(err)
	}

	return &Server{
		producer: producer.NewKafkaProducer(""),
		consumer: c,
		msgCH:    msgCH,
	}
}

func (s *Server) produceMsg() {
	ticket := time.NewTicker(time.Second)

	defer ticket.Stop()

	id := 0
	for t := range ticket.C {
		msg := fmt.Sprintf("hello from kafka, msgID = %d, ts = %s ", id, t.Format("15:20:20"))
		s.producer.Producer(msg)
		id++
	}
}

func (s *Server) handlerMsg(msg string) {
	// db operation
	fmt.Printf("received msg = %s\n", msg)
}

func main() {
	s := NewServer()

	go s.produceMsg()
	for msg := range s.msgCH {
		go s.handlerMsg(msg)
	}

}
