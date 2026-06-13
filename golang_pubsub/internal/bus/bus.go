// Package bus — transport abstraction (playbook §10)
//
// กฎ: business / producer / handler ต้องไม่รู้จัก Pub/Sub หรือ Kafka ตรงๆ
// ครอบ transport ด้วย interface พวกนี้ → สลับ transport = แก้แค่ implementation
// (internal/pubsub/*) ที่เหลือไม่กระทบเลย
package bus

import "context"

// Publisher = ฝั่งส่ง
type Publisher interface {
	// Publish ส่ง payload · key ใช้สำหรับ ordering (ปล่อยว่างได้ถ้าไม่ต้องเรียงลำดับ)
	Publish(ctx context.Context, key string, payload []byte) error
	Close() error
}

// Handler = ฟังก์ชัน process หนึ่ง message
//   - คืน nil  → transport จะ ACK (ถือว่าจบ ไม่ส่งซ้ำ)
//   - คืน err  → transport จะ NACK (ส่งซ้ำภายหลัง — at-least-once)
type Handler func(ctx context.Context, key string, payload []byte) error

// Consumer = ฝั่งรับ
type Consumer interface {
	// Receive บล็อกจน ctx ถูกยกเลิก · เรียก handler ต่อหนึ่ง message
	Receive(ctx context.Context, handler Handler) error
	Close() error
}
