package consumer

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang_kafka/internal/shared"
)

type KafkaConsumer struct {
	Consumer *kafka.Consumer
	topic    string
	msgCH    chan<- string
}

func NewKafkaConsumer(msgCH chan<- string) (*KafkaConsumer, error) {
	cfg := shared.NewKafkaConfig()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Host,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest",
	})

	// topic order confirmed -> delivery -> payment, noticition -> ws
	// you can consume the same topic by 3 different consumers
	// 1 2 3 4 5 6
	// cg1 -> earliest
	// cg2 -> latest
	// cg3 -> 2

	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{cfg.Topic, "^aRegex.*[Tt]opic"}, nil)

	if err != nil {
		return nil, err
	}

	consumer := &KafkaConsumer{
		Consumer: c,
		topic:    cfg.ConsumerGroup,
		msgCH:    msgCH,
	}

	go consumer.readMsgLoop()

	return consumer, err
}

func (c *KafkaConsumer) readMsgLoop() {

	// A signal handler or similar could be used to set this to false to break the loop.

	for {
		msg, err := c.Consumer.ReadMessage(time.Second)
		if err != nil && !err.(kafka.Error).IsTimeout() {
			// fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		payload := msg.Value
		c.msgCH <- string(payload)
	}

	// process msg
}
