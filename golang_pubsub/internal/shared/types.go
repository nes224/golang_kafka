package shared

import (
	"encoding/json"
	"math/rand"
	"time"
)

// Envelope = สัญญากลางของ event (event contract / playbook §5)
// transport-agnostic — ไม่ผูกกับ Kafka หรือ Pub/Sub · เป็นแค่ JSON bytes บนสาย
type Envelope struct {
	EventID   string          `json:"event_id"`
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

func (e *Envelope) Encode() ([]byte, error) { return json.Marshal(e) }

func DecodeEnvelope(data []byte) (*Envelope, error) {
	e := &Envelope{}
	if err := json.Unmarshal(data, e); err != nil {
		return nil, err
	}
	return e, nil
}

// NewEvent — สร้าง event ตัวอย่าง (เหมือน repo.NewEvent ของฝั่ง Kafka)
func NewEvent() *Envelope {
	return &Envelope{
		EventID:   RandID(15),
		EventType: "test_event",
		Timestamp: time.Now().UTC(),
	}
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seeded = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandID(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[seeded.Intn(len(charset))]
	}
	return string(b)
}
