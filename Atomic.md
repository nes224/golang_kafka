dual-write problem classic · และข่าวดีคือ codebase แก้ไว้แล้วด้วย transactional outbox pattern ขอเปิด relay.go ดู logic จริงก่อนตอบให้แม่น

Read
relay.go
เปิดดู relay จริงแล้ว — สรุปสั้นๆ: codebase แก้ปัญหานี้ไว้แล้ว ด้วย Transactional Outbox Pattern เลยไม่มีเคส "DB commit แต่ Kafka ไม่ commit" ตั้งแต่แรก

คำถามที่ 2 ก่อน (ต้นตอ) — dual-write problem
❌ แบบ naive (ที่จะเจอปัญหา)
tx.Commit()              // DB commit แล้ว ✅
kafka.Publish(event)     // ← ถ้าตรงนี้ fail → event หายถาวร · DB commit ไปแล้ว
นี่คือ dual-write — เขียน 2 ที่ (DB + Kafka) ไม่มี transaction ครอบ → ถ้าอันที่สอง fail = ข้อมูลไม่ตรงกัน

✅ แบบที่ใช้จริงในโปรเจกต์ (outbox)
// ใน request — เขียน DB อย่างเดียว · ไม่แตะ Kafka เลย
tx.Exec("INSERT stock_receipts ...")          // business
enqueueEvent(ctx, tx, ...)                     // INSERT event_outbox  ← tx เดียวกัน!
tx.Commit()                                    // ทั้งคู่ atomic

// ทีหลัง — relay goroutine แยกต่างหาก (ไม่อยู่ใน request path)
relay: อ่าน event_outbox → publish Kafka → mark published
กุญแจ: request path ไม่เคยเขียน Kafka เลย · เขียนแค่ event_outbox table (Postgres เดียวกัน · tx เดียวกับ business)

→ เป็นไปไม่ได้ที่ "DB commit แต่ outbox ไม่ commit" เพราะมันคือ tx เดียวกัน · either both commit or both rollback

คำถามที่ 1 — Kafka ล่ม จะทำยังไง
คำตอบ: ไม่ต้องทำอะไรเลย ระบบ degrade graceful เอง

Kafka ล่ม
   │
   ├─ business request ──► ยังทำงานปกติ 100% ✅
   │   (DB tx อย่างเดียว · ไม่รอ Kafka · user ไม่รู้ด้วยซ้ำว่า Kafka ล่ม)
   │
   ├─ event ──► กองอยู่ใน event_outbox (published_at IS NULL) สะสมไปเรื่อยๆ
   │
   └─ relay ──► poll ทุก 2 วิ · publish fail · IncrAttempt · return · รอบหน้าลองใหม่
   
Kafka ฟื้น
   │
   └─ relay drain() ──► FetchUnpublished (ORDER BY created_at เก่าสุดก่อน)
                        → publish ทั้งหมดที่ค้าง → catch up → mark published
                        ไม่มี event หาย · แค่ delay (eventual)
จาก relay.go:45-62 จริง:

FetchUnpublished = WHERE published_at IS NULL ORDER BY created_at ASC → เก่าสุดก่อน
publish fail → IncrAttempt + return (หยุด batch · รักษา order)
publish success → MarkPublished (set published_at)
ผลพลอยได้: at-least-once → consumer ต้อง idempotent
เพราะ relay เป็น at-least-once · มี edge case:

relay: publish Kafka สำเร็จ ✅
       → crash ก่อน MarkPublished ✗
รอบหน้า: fetch row เดิม (ยัง published_at NULL) → publish ซ้ำ → DUPLICATE
→ consumer ต้องกันซ้ำด้วย envelope.ID (UPSERT by PK / เช็ค id ที่เคยเห็น) · ผมเขียนกฎนี้ไว้ใน HR_WAREHOUSE_COMMUNICATION.md §3 แล้ว

⚠️ แต่ — มี 3 gap ที่ยังไม่ harden (เจอตอนอ่าน relay.go)
🔴 1. Poison message blocks ทั้งคิว (อันตรายสุด)
if err := r.publisher.PublishRaw(...); err != nil {
    _ = r.repo.IncrAttempt(ctx, row.ID)
    return  // ← หยุด batch เพื่อรักษา order
}
ถ้ามี 1 event ที่ publish ไม่ได้ถาวร (เช่น topic ไม่มี, payload เกิน max size) → relay return ทุกรอบที่ตัวนี้ → event ที่อยู่ข้างหลังถูก block ตลอดกาล · attempts เพิ่มเรื่อยๆ แต่ไม่มีใครทำอะไร

แก้: เพิ่ม dead-letter — ถ้า attempts > N → ข้ามไป (mark เป็น failed แยก) + alert

🟡 2. Outbox table โตไม่จำกัด
MarkPublished แค่ set published_at · row ที่ส่งแล้วยังอยู่ในตารางตลอด → โตเรื่อยๆ

แก้: janitor ลบ row ที่ published_at < now() - 7 days (เก็บไว้ debug สักพัก)

🟡 3. Multi-replica relay → double publish
ถ้ารัน API หลาย instance → ทุกตัวมี relay goroutine → publish ซ้ำ (at-least-once + idempotent consumer ช่วยได้ แต่เปลือง + อาจ order เพี้ยน)

แก้: SELECT ... FOR UPDATE SKIP LOCKED ตอน fetch · หรือ leader election · (HR มี CRON_MULTI_REPLICA_SAFETY.md อ้างได้)

สรุป
ปัญหา	สถานะ
DB commit แต่ Kafka ไม่ commit (dual-write)	✅ แก้แล้ว — outbox ใน tx เดียวกัน · เป็นไปไม่ได้ที่จะหลุด
Kafka ล่ม	✅ handle แล้ว — degrade graceful · event ค้าง outbox · ฟื้นแล้ว catch-up
Duplicate (at-least-once)	⚠️ ต้องพึ่ง consumer idempotent (กฎมีแล้ว · ต้อง verify consumer ทำจริง)
Poison message block คิว	🔴 ยังไม่ harden
Outbox cleanup	🟡 ยังไม่มี janitor
Multi-replica double publish	🟡 ยังไม่ป้องกัน
อยากให้ผม:

(a) เขียน .md doc เรื่อง outbox resilience + 3 gaps นี้ (เก็บไว้เป็น reference)
(b) fix poison message (dead-letter หลัง N attempts) — gap อันตรายสุด
(c) เพิ่ม outbox janitor + SKIP LOCKED (harden ครบ)
(d) verify consumer idempotency ว่าเช็ค envelope.ID จริงไหม
------------------------------------------------------------------------------------------------------

Atomic = "ทำทั้งหมด หรือไม่ทำเลย" — ไม่มีครึ่งๆ กลางๆ

มาจากคำว่า atom (อะตอม) = แบ่งแยกไม่ได้ · ในบริบท database คือหลายคำสั่งที่ถูกมัดรวมเป็นก้อนเดียว แยกไม่ได้

ตัวอย่างคลาสสิก — โอนเงิน
โอนเงิน 1000 บาท จาก A → B  =  2 คำสั่ง:
  1. หักเงิน A   −1000
  2. เพิ่มเงิน B  +1000
ถ้า ไม่ atomic แล้วไฟดับหลังคำสั่งที่ 1:

A ถูกหัก −1000 ✅
B ยังไม่ได้ +1000 ✗   ← เงิน 1000 หายไปจากระบบ! 💀
ถ้า atomic (มัดเป็น tx เดียว):

ไฟดับตอนไหนก็ตาม → rollback ทั้งคู่
  → A กลับมาเท่าเดิม · B เท่าเดิม
  → ไม่มีเงินหาย
กฎ: ทำสำเร็จทั้ง 2 → commit · ถ้าอันใดอันหนึ่ง fail → rollback ทั้งหมด กลับไปจุดเริ่ม

ในโค้ดจริง — CreateReceipt (รับเข้าสต็อก)
ตอนรับเข้า 1 ใบ มันมีหลายคำสั่งที่ต้อง atomic:

tx, _ := pool.Begin(ctx)         // ── เริ่มก้อน atomic ──
defer tx.Rollback(ctx)            // ถ้าหลุดออกก่อน commit → undo ทั้งหมด

qtx.CreateReceiptHeader(...)      // 1. สร้างหัวใบรับเข้า
for line := range lines {
    qtx.InsertReceiptLine(...)    // 2. insert รายการ
    applyMovement(...)            // 3. UPDATE items.balance += qty
                                  //    + INSERT stock_movement (ledger)
    enqueueEvent(...)             // 4. INSERT event_outbox
}
tx.Commit(ctx)                    // ── จบก้อน · ทุกอย่าง commit พร้อมกัน ──
ถ้า ไม่ atomic แล้ว fail ตรงกลาง:

หัวใบสร้างแล้ว ✅
balance อัปเดตแล้ว ✅
แต่ ledger ยังไม่ insert ✗   ← balance ไม่ตรงกับ movement! ข้อมูลพัง 💀
ถ้า atomic (อยู่ใน tx เดียว):

fail ตรงไหนก็ตาม → tx.Rollback → undo ทุกอย่าง
  → เหมือนไม่เคยกดรับเข้าเลย · ข้อมูลสะอาด
โยงกับเรื่อง outbox เมื่อกี้
ที่ผมบอกว่า "outbox row อยู่ใน tx เดียวกับ business → atomic":

tx.Exec("INSERT stock_receipts ...")   // business
enqueueEvent(ctx, tx, ...)              // INSERT event_outbox  ← tx เดียวกัน
tx.Commit()                             // ← atomic จุดนี้
เพราะ atomic → เป็นไปไม่ได้ที่ "business commit แต่ outbox ไม่ commit" · มันคือก้อนเดียวกัน · commit พร้อมกันเสมอ หรือ rollback พร้อมกันเสมอ

→ นี่คือเหตุผลที่ dual-write problem หายไป

Atomic เป็น "A" ใน ACID
database transaction มี 4 คุณสมบัติ (ACID):

ตัว	ชื่อ	ความหมาย
A	Atomicity	all-or-nothing (ที่กำลังคุย)
C	Consistency	ข้อมูลถูกกฎ (constraint/FK) เสมอ
I	Isolation	tx พร้อมกันไม่กวนกัน (เช่น FOR UPDATE lock)
D	Durability	commit แล้วไม่หาย แม้ไฟดับ
สรุปสั้นสุด
Atomic = มัดหลายคำสั่งเป็นก้อนเดียว · สำเร็จยกก้อน หรือล้มยกก้อน · ไม่มีสภาพครึ่งๆ

ใน Postgres ทำได้ด้วย Begin → คำสั่งหลายอัน → Commit (สำเร็จ) / Rollback (ยกเลิก) — ที่เห็น tx, _ := pool.Begin(ctx) ทุกที่ใน stock/requisition repo นั่นแหละคือการสร้างก้อน atomic