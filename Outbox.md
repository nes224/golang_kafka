Outbox = "กล่องขาออก" — เหมือน outbox ในอีเมล · เป็นตารางใน database ที่จด event ที่รอส่งออก Kafka ไว้ก่อน แล้วมีคนมาเก็บไปส่งทีหลัง

ขอดู schema จริงก่อนอธิบาย:

Ran
Show event_outbox schema
Outbox คืออะไร
เปรียบเทียบกับอีเมล:

เขียนอีเมล → กด Send → ไปอยู่ใน "Outbox" (กล่องขาออก) ก่อน
            → เน็ตมา → ระบบส่งจริง → ย้ายไป "Sent"
ใน warehouse ก็แบบเดียวกัน — แทนที่จะ "ส่ง Kafka ทันที" (ซึ่งอาจล่ม) → เขียนลงตาราง event_outbox ก่อน แล้วมีตัวส่ง (relay) มาเก็บไปส่งทีหลัง

ตาราง event_outbox (ของจริง)
CREATE TABLE event_outbox (
  id           text PRIMARY KEY,    -- id ของ event (idempotency key)
  topic        text NOT NULL,       -- "inventory.stock" → จะส่งไป topic ไหน
  key          text NOT NULL,       -- message key (เช่น item_id)
  payload      jsonb NOT NULL,      -- เนื้อ event (JSON)
  attempts     integer DEFAULT 0,   -- พยายามส่งกี่ครั้งแล้ว (นับ fail)
  created_at   timestamptz,         -- จดเมื่อไหร่
  published_at timestamptz          -- ส่ง Kafka สำเร็จเมื่อไหร่ (NULL = ยังไม่ส่ง) ◄── สำคัญ
);
คอลัมน์ที่เป็นหัวใจ = published_at:

NULL = ยังอยู่ใน "กล่องขาออก" รอส่ง
มีค่า = ส่ง Kafka สำเร็จแล้ว (เหมือนย้ายไป "Sent")
flow เต็ม — 3 จังหวะ
จังหวะ 1 · WRITE (ตอน CreateReceipt · ใน business tx)
─────────────────────────────────────────────────────
  enqueueEvent(ctx, tx, ...)
    → INSERT event_outbox (published_at = NULL)  ◄── จดลงกล่องขาออก
    → อยู่ tx เดียวกับ business · atomic
  tx.Commit()  → business + outbox commit พร้อมกัน
จังหวะ 2 · RELAY อ่าน (goroutine แยก · poll ทุก 2 วิ)
─────────────────────────────────────────────────────
  FetchUnpublished → SELECT * WHERE published_at IS NULL  ◄── หยิบที่ยังไม่ส่ง
  ORDER BY created_at ASC (เก่าสุดก่อน)
จังหวะ 3 · RELAY ส่ง + mark
─────────────────────────────────────────────────────
  publisher.PublishRaw(topic, key, payload)  → ส่ง Kafka จริง
  MarkPublished → UPDATE SET published_at = now()  ◄── ย้ายไป "Sent"
ทำไมต้องมี outbox (ไม่ส่ง Kafka ตรงๆ)
❌ ถ้าส่ง Kafka ตรงๆ (ไม่มี outbox)
tx.Commit()              // business commit ✅
kafka.Publish(event)     // ← Kafka ล่ม → event หายถาวร 💀
ปัญหา: DB กับ Kafka เป็นคนละระบบ มัด transaction ร่วมกันไม่ได้ → ถ้าอันที่สองพัง = ข้อมูลไม่ตรง (dual-write problem)

✅ ถ้ามี outbox
tx.Exec("INSERT business")
tx.Exec("INSERT event_outbox")   // ← เขียน DB เดียวกัน · tx เดียวกัน
tx.Commit()                       // atomic · ไม่หลุดแน่
// Kafka ส่งทีหลังโดย relay (ไม่อยู่ใน request)
เพราะ outbox อยู่ใน Postgres เดียวกับ business → มัด tx ร่วมกันได้ → atomic

ประโยชน์ที่ได้
สถานการณ์	ผลลัพธ์
Kafka ล่ม	request ยังทำงาน · event กองใน outbox · ฟื้นแล้ว relay ส่งให้ครบ
business commit แต่ Kafka ส่งไม่ได้	เป็นไปไม่ได้ — event อยู่ใน outbox แล้ว (commit ไปพร้อม business) · relay จะส่งให้เอง
relay crash ตอนส่ง	event ยังอยู่ใน outbox (published_at NULL) · relay ตัวใหม่มาส่งต่อ
ดูข้อมูลจริงใน outbox
ถ้าเปิด DB ดูตาราง event_outbox จะเห็นประมาณนี้:

id	topic	key	payload	published_at
abc-1	inventory.stock	item-99	{type:"stock.received",qty:50,...}	2026-06-12 10:30 ✅ ส่งแล้ว
abc-2	inventory.requisitions	req-5	{type:"requisition.created",...}	2026-06-12 10:31 ✅
abc-3	inventory.stock	item-12	{type:"stock.issued",qty:-8,...}	NULL ⏳ รอส่ง (Kafka ล่ม?)
โค้ดที่เกี่ยวข้อง
หน้าที่	ไฟล์	function
เขียนลง outbox (ใน tx)	outbox.go:14	enqueueEvent
อ่าน + mark	outbox.go:47	OutboxRepo (Fetch/Mark/IncrAttempt)
ตัวส่ง (poll → publish)	relay.go:30	Relay.Run / drain
สรุปประโยคเดียว
Outbox = ตารางกลางใน DB ที่จด event ไว้ก่อน (พร้อม business ใน tx เดียว = atomic) แล้ว relay มาเก็บไปส่ง Kafka ทีหลัง — แก้ปัญหา DB กับ Kafka มัด transaction ร่วมกันไม่ได้

ชื่อเต็มของเทคนิคนี้ = Transactional Outbox Pattern (industry standard · Netflix, Uber, ฯลฯ ใช้กันหมด)