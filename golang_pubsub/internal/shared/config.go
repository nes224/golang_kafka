package shared

import "os"

// Config — ตั้งค่าทั้งหมดผ่าน env (ไม่ hardcode credential เหมือนฝั่ง Kafka)
//
// เทียบ Kafka:
//   ProjectID       ~ ไม่มีใน Kafka (GCP project scope)
//   TopicID         = topic (เหมือนกัน)
//   SubscriptionID  ~ consumer group (subscription คือ "กลุ่มผู้รับ" ของ Pub/Sub)
//   EmulatorHost    ~ bootstrap.servers (ปลายทางที่ client ต่อ)
type Config struct {
	ProjectID      string
	TopicID        string
	SubscriptionID string
	EmulatorHost   string
	EnableOrdering bool
}

func NewConfig() *Config {
	return &Config{
		ProjectID:      env("PUBSUB_PROJECT_ID", "local-project"),
		TopicID:        env("PUBSUB_TOPIC", "events-topic"),
		SubscriptionID: env("PUBSUB_SUBSCRIPTION", "events-sub"),
		// ถ้าตั้งค่านี้ไว้ client library จะต่อ emulator อัตโนมัติ (ไม่ต้องมี GCP credential)
		EmulatorHost:   os.Getenv("PUBSUB_EMULATOR_HOST"),
		EnableOrdering: os.Getenv("PUBSUB_ORDERING") == "true",
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
