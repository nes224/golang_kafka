# Study Guide — เส้นทางเรียน Kafka จากศูนย์ถึง production patterns

ไฟล์นี้คือ **ลำดับเรียนทีละขั้น** ร้อยเอกสารทั้งหมดในโปรเจกต์เข้าด้วยกัน — บอกว่าอ่านอะไรก่อนหลัง แต่ละขั้นเรียนเพื่ออะไร และมีคำถามเช็คตัวเองว่าเข้าใจจริงไหมก่อนไปต่อ

> วิธีใช้: เรียงจากบนลงล่าง แต่ละขั้นมี **เป้าหมาย → อ่านอะไร → concept สำคัญ → เช็คตัวเอง** อ่านจบขั้นแล้วลองตอบคำถามท้ายขั้น ถ้าตอบได้ค่อยไปขั้นถัดไป ถ้ายังก็กลับไปอ่านซ้ำ

> เวลาโดยประมาณ: Stage 0-2 (พื้นฐาน) ~2-3 ชม. · Stage 3-4 (กลไกลึก) ~3-4 ชม. · Stage 5 (patterns) ~4-6 ชม. ไม่ต้องรีบ เข้าใจแต่ละขั้นสำคัญกว่าเร็ว

---

## ภาพรวมเส้นทาง

```
Stage 0  ปูศัพท์ + ภาพรวม           ← รู้ว่ากำลังเรียนอะไร
Stage 1  Core mechanics             ← topic/partition/offset/commit คืออะไร
Stage 2  อ่านโค้ดให้ออก             ← โค้ดจริงทำงานยังไง ทีละบรรทัด
Stage 3  Message lifecycle          ← message 1 ตัวเดินทางยังไง (รับ→process→commit)
Stage 4  Rebalance + scaling        ← ทำไม scale หลาย pod ได้ (หัวใจ production)
Stage 5  Correctness patterns       ← Inbox/Outbox/CDC (ของจริงที่ใช้ใน ERP)
─────────────────────────────────────────────────────────────────
จบ Stage 5 = เข้าใจ event-driven microservices พอจะออกแบบ/รีวิวเองได้
ขั้นถัดไป (ยังไม่มีบทเรียน): Phase 2-4 production hardening ใน LEARNING_ROADMAP.md
```

---

## Stage 0 — ปูศัพท์ + ภาพรวม

**เป้าหมาย:** รู้ว่าโปรเจกต์นี้คืออะไร ทำอะไรได้ และมีศัพท์อะไรบ้างที่จะเจอ

**อ่าน:**
1. `HANDOFF.md` — ทั้งไฟล์ (สั้น) รู้ว่าโปรเจกต์อยู่จุดไหน ทำอะไรได้แล้วบ้าง
2. `KAFKA_GLOSSARY.md` — อ่านผ่านๆ ทั้ง 9 หมวด ยังไม่ต้องจำ แค่ให้คุ้นว่ามีคำพวกนี้ (เปิดกลับมาดูได้ตลอด)

**Concept สำคัญ:**
- Kafka = ท่อส่ง message แบบ pub/sub ที่จำได้ว่าใครอ่านถึงไหน (offset)
- โปรเจกต์นี้ = producer ยิง event → consumer อ่าน process → ลง DB → commit

**เช็คตัวเอง:**
- ตอบได้ไหมว่า topic / partition / offset / consumer group ต่างกันยังไง (1 ประโยคต่ออัน)
- producer กับ consumer ใครยิงใครอ่าน?

---

## Stage 1 — Core mechanics

**เป้าหมาย:** เข้าใจ "กลไก" ของ Kafka — ทำไมต้องมี partition ทำไมต้อง commit เอง

**อ่าน:**
1. `ARCHITECTURE.md §1-2` — ภาพรวมระบบ + data flow (ดู diagram ให้เข้าใจว่าอะไรต่อกับอะไร)
2. `ARCHITECTURE.md §5.1-5.3` — sequential commit, idempotent, manual commit (3 กลไกหลัก)

**Concept สำคัญ:**
- **Partition = เลนขนาน** — แบ่ง topic เป็นหลายเลนเพื่ออ่านพร้อมกันได้ + scale
- **Offset = บุ๊กมาร์ก** ต่อ partition — บอกว่าอ่านถึงไหน
- **Manual commit** — commit เองหลัง process เสร็จจริง (ไม่ใช่อัตโนมัติ) → message ไม่หาย
- **Sequential commit** — commit ได้แค่ offset ที่เสร็จต่อเนื่องไม่มีรู (เจอตัวค้าง = หยุด)

**เช็คตัวเอง:**
- ถ้า process แบบขนานแล้ว offset 5 เสร็จก่อน offset 3 — commit ได้ถึงไหน? ทำไม?
- at-least-once กับ at-most-once ต่างกันยังไง? โปรเจกต์นี้ใช้แบบไหน?

---

## Stage 2 — อ่านโค้ดให้ออก

**เป้าหมาย:** map concept → โค้ดจริง รู้ว่าแต่ละไฟล์ทำอะไร

**อ่าน:**
1. `CODE_WALKTHROUGH.md` — ทั้งไฟล์ ไล่ตาม build order (Step 1→10) เปิดโค้ดจริงคู่ไปด้วย
2. เปิดไฟล์โค้ดจริงตามไป: `kafka-config.go` → `event-repo.go` → `types.go` → `producer.go` → `consumer.go` → `main.go`

**Concept สำคัญ:**
- **build order** — config (ไม่พึ่งใคร) → repo (DB) → types → producer → consumer → main (ประกอบ)
- **TxClosure** — generic helper ครอบ transaction (begin/commit/rollback อัตโนมัติ)
- **goroutine + channel** — consumer อ่าน → ส่งเข้า `msgCH` → main fan-out `go handleMsg`

**เช็คตัวเอง:**
- ไล่ได้ไหมว่า event 1 ตัวจาก `produceMsg()` ไปถึง `saveToDB()` ผ่านอะไรบ้าง?
- `TxClosure` ถ้า insert พลาดจะเกิดอะไรขึ้น? (commit หรือ rollback?)

---

## Stage 3 — Message lifecycle

**เป้าหมาย:** เห็นภาพชีวิต message 1 ตัวครบ loop รวมถึง state machine (Pending/Success/Error)

**อ่าน:**
1. `MESSAGE_LIFECYCLE.md` — ทั้งไฟล์ โฟกัสที่ flowchart รวม + ช่วง A/B/C
2. `parition-state.go` — โค้ดจริงของ `findLatestToCommit()` (หัวใจ sequential commit)

**Concept สำคัญ:**
- **3 state:** Pending (เพิ่งอ่าน) → Success/Error (process เสร็จ)
- **commit loop** วิ่งแยกทุก N วิ → scan หา offset ต่อเนื่องที่เสร็จ → commit
- **เจอ Pending = กำแพง** — หยุด commit ตรงนั้น (กันข้าม message ที่ยังไม่เสร็จ)
- **Error ก็ผ่านได้** — Error ถือว่า "จัดการแล้ว" ไม่บล็อก commit (ไม่งั้นติดถาวร)

**เช็คตัวเอง:**
- ทำไม Pending บล็อก commit แต่ Error ไม่บล็อก?
- ถ้า consumer crash ตอน message Pending อยู่ — รอบหน้าจะเกิดอะไร? (อ่านซ้ำไหม?)

> 💡 เกร็ด: ท้ายไฟล์ `MESSAGE_LIFECYCLE.md` มีส่วน "ความต่างจากโค้ดปัจจุบัน" ที่เขียนตอนโค้ดยังเก่า — ตอนนี้โค้ดมี per-partition state + enum ครบแล้ว ส่วนนั้น outdated อ่านข้ามได้

---

## Stage 4 — Rebalance + scaling (หัวใจ production)

**เป้าหมาย:** เข้าใจว่าทำไมรันหลาย instance พร้อมกันได้ และ scale in/out ทำงานยังไง

**อ่าน:**
1. `REBALANCE.md` — ทั้งไฟล์ (§1-9) โฟกัส §5 (assign) + §6 (revoke = หัวใจความปลอดภัย)
2. `ARCHITECTURE.md §8` — scaling, กฎ partition/consumer 5 ข้อ, k8s ↔ Kafka

**Concept สำคัญ:**
- **Rebalance** — Kafka แจก partition ใหม่เมื่อ consumer เพิ่ม/หาย
- **commit-before-revoke** — commit งานที่เสร็จก่อนปล่อย partition (ไม่งั้น pod ใหม่อ่านซ้ำเยอะ)
- **cooperative-sticky** — ปล่อยเฉพาะ partition ที่ต้องย้าย (downtime น้อย)
- **partition = เพดาน** — scale consumer ได้สูงสุด = จำนวน partition (เกินนั้น IDLE)

**เช็คตัวเอง:**
- topic มี 4 partition เปิด consumer 6 ตัว — ทำงานจริงกี่ตัว? ที่เหลือทำอะไร?
- ทำไมต้อง commit ก่อน revoke? ถ้าไม่ทำจะเกิดอะไร?
- **ลองจริง:** เปิด `go run ./cmd` 2 terminal พร้อมกัน ดู log `✅ Assigned` / `❌ Revoking` แล้วปิดตัวนึง ดู partition ย้าย

---

## Stage 5 — Correctness patterns (ของจริงที่ใช้ใน ERP)

**เป้าหมาย:** เข้าใจ 3 pattern ที่ทำให้ event-driven microservices ถูกต้องจริง — และเป็นสิ่งที่ ERP ใช้อยู่

เรียง 3 บทนี้ **ตามลำดับ** เพราะต่อยอดกัน:

### 5.1 Inbox — กันประมวลผลซ้ำ (ขาเข้า)
**อ่าน:** `INBOX_PATTERN.md` ทั้งไฟล์
**Concept:** at-least-once = ซ้ำแน่ๆ → ต้องกันที่ business key (event_id) ไม่ใช่ offset · `ON CONFLICT DO NOTHING` atomic แทน Get-ก่อน-Insert · แยกตาราง inbox ออกจาก business
**เช็คตัวเอง:** ทำไมต้องแยกตาราง inbox ไม่ใช้ business table? · ทำไม `ON CONFLICT` ดีกว่า Get-ก่อน-Insert ในเรื่อง race?

### 5.2 Outbox — กัน event หาย (ขาออก)
**อ่าน:** `OUTBOX_PATTERN.md` ทั้งไฟล์ — บทนี้เป็น blueprint พร้อม implement มี checklist 10 ขั้น
**Concept:** dual-write พังได้ → เขียน event ลงตาราง outbox ใน tx เดียวกับ business · relay poll → publish → mark sent · `FOR UPDATE SKIP LOCKED` ให้รัน relay หลายตัว · `acks=all` + idempotence กัน event หาย/ซ้ำตอนส่ง
**เช็คตัวเอง:** ทำไมยิง Kafka ตรงๆ จาก business ไม่ได้? · relay ควร publish ก่อน mark หรือ mark ก่อน publish? ทำไม? · ทำไม outbox อย่างเดียวยังไม่พอ ต้องมี inbox ด้วย?

### 5.3 CDC / Debezium — ทางเลือก/upgrade ของ Outbox
**อ่าน:** `CDC_DEBEZIUM.md` ทั้งไฟล์ โฟกัส §6 (Outbox vs CDC decision tree) + §7 (hybrid)
**Concept:** Debezium อ่าน WAL ของ Postgres → ยิง change เข้า Kafka อัตโนมัติ ไม่ต้องเขียน relay · Outbox vs CDC เลือกยังไง · Outbox+CDC hybrid (EventRouter อ่านตาราง outbox)
**เช็คตัวเอง:** เมื่อไหร่ควรใช้ Outbox เมื่อไหร่ควรใช้ CDC? · ERP ควรย้ายไป Debezium ตอนนี้ไหม? เพราะอะไร?

**ภาพรวม 3 บทประกอบกัน:**
```
Service A: business + OUTBOX ──relay──▶ Kafka ──consume──▶ INBOX + business : Service B
           (กัน event หาย)                              (กัน process ซ้ำ)
           └──────── effectively-once ทั้ง pipeline ────────┘
           CDC = ทางเลือกแทน relay (Debezium อ่าน WAL/ตาราง outbox)
```

---

## หลังจบ Stage 5 — ไปไหนต่อ

ตอนนี้เข้าใจ correctness ครบแล้ว ขั้นถัดไปคือ "ทำให้ทนทาน + ขึ้น production จริง" — **ยังไม่มีบทเรียนเขียนไว้** แต่ `LEARNING_ROADMAP.md` วางแผนไว้:

```
Phase 2  Production hardening (Tier 1)  ← ต้องทำก่อน ship
         security (DB cred → env), graceful shutdown, replication factor ≥3, error/retry
Phase 3  Production essential (Tier 2)  ← ก่อนรับ traffic
         DLQ, producer reliability config, observability (consumer lag), tracing
Phase 4  Maturity (Tier 3)              ← ทนทานระยะยาว
         schema registry, TLS+SASL, testing, alerting
```

อ่าน `LEARNING_ROADMAP.md` Phase 2-4 + `ARCHITECTURE.md §7` (known issues) เพื่อรู้ว่าโค้ดปัจจุบันยังขาดอะไร

---

## แผนที่เอกสารทั้งหมด (อ้างอิงเร็ว)

| ไฟล์ | ใช้ตอน Stage | เนื้อหา |
|---|---|---|
| `HANDOFF.md` | 0 | จุดเริ่ม — สถานะ + วิธีรัน |
| `KAFKA_GLOSSARY.md` | 0 (+เปิดตลอด) | คำศัพท์ 9 หมวด |
| `ARCHITECTURE.md` | 1, 4 | ภาพรวม + mechanics + scaling + known issues |
| `CODE_WALKTHROUGH.md` | 2 | โค้ดทีละบรรทัด (build order) |
| `MESSAGE_LIFECYCLE.md` | 3 | flow ชีวิต message + state machine |
| `REBALANCE.md` | 4 | rebalance เชิงลึก (assign/revoke) |
| `INBOX_PATTERN.md` | 5.1 | กันซ้ำขาเข้า (idempotent consumer) |
| `OUTBOX_PATTERN.md` | 5.2 | กันหายขาออก (blueprint + checklist) |
| `CDC_DEBEZIUM.md` | 5.3 | CDC/Debezium + Outbox vs CDC |
| `LEARNING_ROADMAP.md` | หลัง 5 | แผน Phase 2-4 |
| `HR_KAFKA_WIRING.md` | 5 (อ้างอิง) | outbox จริงใน ERP |

> เคล็ดลับ: ถ้าหลงหรือลืม concept ไหน เปิด `KAFKA_GLOSSARY.md` ก่อน แล้วค่อยกลับมาที่ไฟล์เชิงลึกของ stage นั้น
