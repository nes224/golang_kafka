# EDA vs Broker — Kafka / Google Cloud Pub/Sub (อ่านเข้าใจง่าย)

> สรุปคอนเซปต์: **Event-Driven Architecture (EDA)** กับ **message broker (Kafka / Pub/Sub)** คนละชั้นกัน
> โยงกับระบบจริง HAP ERP (`erp_kafka_module/bus`) · อัปเดต: 2026-06-18

---

## 0. TL;DR

- **Kafka / Pub/Sub ≠ EDA โดยตัวมันเอง** — มันคือ "ท่อ" (transport/broker) ที่ **ทำให้เกิด** EDA
- **EDA = รูปแบบสถาปัตยกรรม (pattern)** · **Kafka/Pub/Sub = เครื่องมือ (infrastructure)**
- ทั้งคู่เป็น messaging/streaming platform ที่ **ใช้สร้าง EDA ได้ทั้งคู่** → เลยสลับกันได้ด้วย env ตัวเดียว

---

## 1. คนละชั้นกัน

| | คืออะไร | ตัวอย่าง |
|---|---|---|
| **Event-Driven Architecture (EDA)** | **pattern / วิธีออกแบบ** — service คุยกันด้วย event แบบ async, decoupled (ผู้ส่งไม่รู้/ไม่รอผู้รับ) | "เมื่อของไม่พอ → ยิง event → ฝ่ายจัดซื้อสร้าง PR เอง" |
| **Message broker / transport** | **เครื่องมือ** ที่ขนส่ง event ระหว่าง service | **Kafka**, **Google Cloud Pub/Sub**, RabbitMQ, NATS |

> พูดให้ตรง: **"Kafka และ Pub/Sub เป็น messaging/streaming platform ที่ใช้สร้าง event-driven architecture"** — ไม่ใช่ตัวมันเองเป็น EDA

เปรียบเทียบ:
- **EDA** = "ระบบไปรษณีย์" (วิธีทำงาน: ฝากจดหมาย ไม่ต้องรอผู้รับ)
- **Kafka / Pub/Sub** = "รถขนส่ง/สายพาน" ที่เอาจดหมายไปส่ง (จะใช้รถยี่ห้อไหนก็ได้ ระบบไปรษณีย์เหมือนเดิม)

---

## 2. พิสูจน์จากระบบ HAP เอง

เหตุผลที่ **สลับ Kafka ↔ Pub/Sub ได้ด้วย `EVENT_TRANSPORT=kafka|pubsub` ตัวเดียว** โดย architecture ไม่เปลี่ยนเลย:

```
┌─────────────────── EDA (pattern · transport-agnostic) ───────────────────┐
│  business write → event_outbox (atomic · กัน dual-write)                   │
│       → relay loop → bus.PublishRaw()                                      │
│  consumer → processed_events (idempotent dedup) → react                   │
│  topics / event types / saga (correlation_id)                             │
└───────────────────────────────┬──────────────────────────────────────────┘
                                 │  เปลี่ยนแค่ "leaf" ที่ขน byte
                    ┌────────────┴────────────┐
                    ▼                         ▼
            [ Kafka impl ]            [ Pub/Sub impl ]   ← bus/publisher.go switch
```

- **EDA = ส่วนที่ transport-agnostic** → topics, event types, outbox, idempotent consumer, decoupled services (เช่น `shortfall.created` → procurement สร้าง PR → `po.received` → inventory auto-receipt)
- **Kafka/Pub/Sub = แค่ leaf ที่ขน byte**

> ถ้า Kafka/Pub/Sub *เป็น* EDA จริง การสลับท่อต้องรื้อ architecture — แต่เราสลับได้เพราะมันเป็นแค่ **implementation ของชั้น transport** ใต้ EDA

โค้ดจริง (`erp_kafka_module/bus/publisher.go`):
```go
func NewPublisher(brokers) Publisher {
    if transport == "pubsub" { return NewPubSubPublisher(...) }  // Pub/Sub
    return NewKafkaPublisher(brokers)                            // Kafka
}
```

---

## 3. กลไกที่ทำให้ EDA เชื่อถือได้ (อยู่ "เหนือ" ท่อ · ไม่ขึ้นกับ Kafka/Pub/Sub)

| กลไก | แก้ปัญหาอะไร | อยู่ที่ไหน |
|---|---|---|
| **Outbox** | dual-write inconsistency (เขียน DB + ยิง event ไม่ atomic) | SQL table `event_outbox` + relay |
| **Inbox / idempotent** | at-least-once delivery → กัน process ซ้ำ | SQL table `processed_events` (เช็ค `event_id`) |
| **Relay (singleton)** | อ่าน outbox → publish → mark sent | background loop (ต้องรัน 1 instance · advisory lock) |
| **Saga / correlation_id** | order ข้าม service / flow ยาว | envelope field |

> ทั้งหมดนี้ = EDA design · **ไม่เปลี่ยน**ไม่ว่าจะใช้ Kafka หรือ Pub/Sub

---

## 4. ความต่างย่อยของ 2 ท่อ (เผื่ออยากรู้)

| | **Kafka** | **Google Cloud Pub/Sub** |
|---|---|---|
| ประเภท | distributed **log / streaming** | managed **message bus** |
| เด่น | event เก็บเป็น log · **replay ได้** · partition · stream processing | serverless · ไม่มี cluster · ack/nack · DLQ ในตัว |
| semantics | pub-sub + log (event sourcing ได้) | pub-sub (messaging เป็นหลัก) |
| ใช้ทำ EDA | ✅ ได้ (+ replay/streaming) | ✅ ได้ |
| เข้ากับ runtime | K8s / GKE (long-lived pod) | Cloud Run (serverless · push sub) |

→ ทั้งคู่ **ใช้ทำ EDA ได้** · Kafka แถม log/replay มากกว่า · Pub/Sub ฝั่ง ops/serverless ดีกว่า

---

## 5. สรุป

1. ✅ **ถูกต้องในเจตนา** — ทั้ง Kafka และ Pub/Sub ใช้ทำ event-driven ได้
2. ✏️ **ให้แม่น** — ทั้งคู่เป็น **broker/transport ที่ implement EDA** · **EDA คือ pattern, Kafka/Pub/Sub คือเครื่องมือ**
3. 🎯 **ระบบ HAP** = EDA (pattern) + outbox/inbox/relay (กลไก) + Kafka **หรือ** Pub/Sub (ท่อ ที่สลับได้ด้วย `EVENT_TRANSPORT`)

> หัวใจ: ออกแบบ EDA ให้ดี (decoupled + outbox + idempotent) แล้ว **ท่อเป็นแค่รายละเอียดที่เปลี่ยนทีหลังได้**