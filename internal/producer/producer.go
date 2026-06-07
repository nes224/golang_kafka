package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang_kafka/internal/shared"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer(topic string) *KafkaProducer {
	cfg := shared.NewKafkaConfig()
	if topic == "" {
		topic = cfg.Topic
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
	if err != nil {
		panic(err)
	}

	// ห้าม defer p.Close() ตรงนี้! เพราะ constructor จะ return ทันที
	// → producer ถูกปิดก่อนได้ใช้ ให้ปิดตอน shutdown ผ่าน method Close() แทน

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return &KafkaProducer{
		producer: p,
		topic:    topic,
	}
}

func (p *KafkaProducer) Producer(msg string) {
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)

	if err != nil {
		fmt.Printf("error producing msg := %v\n", err)
	}

}

// Close ปิด producer ให้เรียกตอน graceful shutdown (เช่นใน main ก่อนจบโปรแกรม)
// Flush รอ message ที่ค้างใน queue ให้ส่งครบก่อน (รอสูงสุด 5 วินาที) แล้วค่อยปิด
func (p *KafkaProducer) Close() {
	p.producer.Flush(5000)
	p.producer.Close()
}
