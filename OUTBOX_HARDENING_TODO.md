# Outbox Hardening — TODO

> status: **core outbox ทำเสร็จแล้ว · เหลือ hardening 3 อัน (production-grade)**
> context: `EVENT_DRIVEN_PLAYBOOK.md` §11–12 · reference impl: `relay.go` + `outbox.go`
> ความสำคัญ: ไม่กระทบ correctness ของ pattern (gan dual-write + Kafka ล่ม ทำได้แล้ว) · แต่จำเป็นก่อน scale/production จริง
>
> 🔁 **Fleet note (2026-06-13):** dead-letter + janitor (TODO1+2) port ไป **HR relay** ด้วยแล้ว (`erp_hr_module_be/internal/adapter/eventpub/relay.go` + migration `0098_outbox_dead_letter.up.sql`) · HR relay ยัง **unwired** (latent · จะ active ตอน wire ตาม `HR_KAFKA_WIRING.md`) แต่เกิดมาพร้อม hardening แล้ว · build+vet ทั้ง 3 module ผ่าน

## สถานะปัจจุบัน (baseline)

✅ **ทำแล้ว (core):**
- `event_outbox` table + partial index (`idx_event_outbox_unpublished`)
- `enqueueEvent(ctx, tx, …)` — atomic write ใน business tx (5 จุด)
- `relay.go` — poll (2s) → publish → mark published · หยุด batch เมื่อ fail (รักษา order)
- `OutboxRepo` — `FetchUnpublished` / `MarkPublished` / `IncrAttempt`
- bus abstraction (`bus/publisher.go`)

สถานะ hardening (อัปเดต 2026-06-13):
1. ✅ **Dead-letter (poison message)** — code-complete (build+vet ผ่าน · รอ verify runtime ด้วย Kafka จริง)
2. ✅ **Outbox janitor (cleanup)** — code-complete (ทางเลือก A · goroutine ใน relay)
3. ❌ 🟡 **Multi-replica safety (double-publish)** — ยังไม่ทำ (เลื่อน · ตอนนี้ deploy single instance)

> ✅ TODO 1+2 ทำใน `0006_outbox_dead_letter.sql` + `outbox.go` + `relay.go` (มี dead-letter + janitor ticker) · ดูล่าง

---

## 🔴 TODO 1 · Dead-Letter (poison message) — **อันตรายสุด · ทำก่อน**

### ปัญหา
`relay.drain()` ทำ `return` ทุกครั้งที่ publish fail (เพื่อรักษา order):
```go
if err := r.publisher.PublishRaw(...); err != nil {
    _ = r.repo.IncrAttempt(ctx, row.ID)
    return   // ◄── หยุด batch
}
```
ถ้ามี event 1 ตัว publish **ไม่ได้ถาวร** (topic ไม่มี · payload เกิน size · serialization พัง) → relay เจอตัวนี้ทุกรอบ → `return` ทุกรอบ → **event ที่อยู่ข้างหลังถูก block ตลอดกาล** · `attempts` เพิ่มเรื่อยๆ แต่ไม่มีใครทำอะไร

### Fix
1. **migration** — เพิ่ม column บอกสถานะ dead-letter
   ```sql
   -- db/migrations/00XX_outbox_dead_letter.sql
   ALTER TABLE event_outbox ADD COLUMN IF NOT EXISTS failed_at timestamptz;
   ALTER TABLE event_outbox ADD COLUMN IF NOT EXISTS last_error text;
   -- partial index ต้อง exclude dead-letter ด้วย
   DROP INDEX IF EXISTS idx_event_outbox_unpublished;
   CREATE INDEX idx_event_outbox_unpublished
     ON event_outbox (created_at) WHERE published_at IS NULL AND failed_at IS NULL;
   ```
   → sync `db/sqlc/schema.sql` ด้วย (กฎ sqlc)

2. **OutboxRepo** — เพิ่ม method
   ```go
   func (r *OutboxRepo) MarkDeadLetter(ctx context.Context, id, errMsg string) error {
       _, err := r.pool.Exec(ctx,
           "UPDATE event_outbox SET failed_at = now(), last_error = $2 WHERE id = $1", id, errMsg)
       return err
   }
   ```
   + แก้ `FetchUnpublished` query → `WHERE published_at IS NULL AND failed_at IS NULL`

3. **relay.drain()** — เช็ค attempts ก่อน return
   ```go
   const maxAttempts = 10
   if err := r.publisher.PublishRaw(ctx, row.Topic, row.Key, row.Payload); err != nil {
       if row.Attempts+1 >= maxAttempts {
           r.repo.MarkDeadLetter(ctx, row.ID, err.Error())  // ย้ายออก · ไม่ block
           log.Printf("outbox relay: DEAD-LETTER id=%s after %d attempts: %v", row.ID, maxAttempts, err)
           continue   // ◄── ข้ามไป · event ข้างหลังไปต่อได้
       }
       r.repo.IncrAttempt(ctx, row.ID)
       return   // ยังไม่ถึง max → หยุด batch retry รอบหน้า (รักษา order)
   }
   ```

### Acceptance
- [x] migration (`0006_outbox_dead_letter.sql`) + schema.sql sync (เพิ่ม `failed_at`/`last_error` + partial index exclude dead-letter)
- [x] poison message → mark `failed_at` หลัง `maxAttempts=10` (relay `drain` · `MarkDeadLetter`) — *รอ verify runtime*
- [x] event ที่อยู่หลัง poison ยัง publish ได้ (drain ใช้ `continue` แทน `return` หลัง dead-letter)
- [x] log ตอน dead-letter (`log.Printf("outbox relay: DEAD-LETTER ...")`)
- [x] `go build && go vet` ผ่าน
- [ ] ⏳ verify runtime: ยิง poison จริงด้วย Kafka broker → เห็น `failed_at` set + คิวไม่ block

> ⚠️ trade-off: `continue` ข้าม poison = **เสีย order** เฉพาะ key ของ poison นั้น · ยอมรับได้ (ดีกว่า block ทั้งคิว) · event key อื่นไม่กระทบ

---

## 🟡 TODO 2 · Outbox Janitor (cleanup) — table โตไม่จำกัด

### ปัญหา
`MarkPublished` แค่ set `published_at` · row ที่ส่งแล้วยังอยู่ในตารางตลอด → โตเรื่อยๆ → query ช้าลง · disk เปลือง

### Fix
**ทางเลือก A · goroutine ใน relay (ง่ายสุด)**
```go
// ใน relay.Run — เพิ่ม janitor ticker แยก (รันวันละครั้งพอ)
janitor := time.NewTicker(24 * time.Hour)
// ...
case <-janitor.C:
    n, _ := r.repo.DeletePublishedBefore(ctx, 7*24*time.Hour)
    log.Printf("outbox janitor: deleted %d published rows", n)
```
```go
func (r *OutboxRepo) DeletePublishedBefore(ctx context.Context, age time.Duration) (int64, error) {
    ct, err := r.pool.Exec(ctx,
        "DELETE FROM event_outbox WHERE published_at IS NOT NULL AND published_at < now() - $1::interval",
        age.String())
    if err != nil { return 0, err }
    return ct.RowsAffected(), nil
}
```

**ทางเลือก B · external cron / pg_cron** (ถ้ามี scheduler อยู่แล้ว)
```sql
DELETE FROM event_outbox WHERE published_at < now() - interval '7 days';
```

### Acceptance
- [x] row ที่ `published_at` เกิน 7 วัน ถูกลบ (`DeletePublishedBefore` · janitor ticker 24h ใน `relay.Run`)
- [x] **ไม่ลบ** row ที่ `failed_at` (query กรอง `published_at IS NOT NULL` เท่านั้น) · **ไม่ลบ** unpublished
- [ ] ⏳ ลบเป็น batch (`LIMIT` + loop) — ยังไม่ทำ · ทำเมื่อ volume สูงพอจะ lock นาน (ตอนนี้ single DELETE พอ)

> เก็บ 7 วันเป็น default (debug/audit) · ปรับได้ตามต้องการ

---

## 🟡 TODO 3 · Multi-Replica Safety — double-publish

### ปัญหา
ถ้ารัน API หลาย instance (horizontal scale) → ทุก instance มี relay goroutine → ทุกตัว `FetchUnpublished` ได้ row เดียวกัน → **publish ซ้ำ**
- correctness ไม่พัง (consumer idempotent อยู่แล้ว) · แต่เปลือง + อาจ order เพี้ยนระหว่าง replica

### Fix — เลือก 1
**ทางเลือก A · SELECT FOR UPDATE SKIP LOCKED (แนะนำ · contained)**
```go
func (r *OutboxRepo) FetchAndLockUnpublished(ctx context.Context, tx pgx.Tx, limit int) ([]OutboxRow, error) {
    rows, err := tx.Query(ctx, `
        SELECT id, topic, key, payload::text FROM event_outbox
        WHERE published_at IS NULL AND failed_at IS NULL
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED`, limit)   // ◄── replica อื่นข้าม row ที่ถูก lock
    // ... scan ...
}
```
→ `drain` เปิด tx · fetch+lock · publish · mark · commit (lock ปล่อยตอน commit) · replica อื่นหยิบคนละ row

> ⚠️ trade-off: ถือ row lock ระหว่าง publish (network call) · batch เล็ก (100) + interval สั้น → ยอมรับได้ · ถ้า publish ช้ามากค่อยลด batch

**ทางเลือก B · Leader election (1 relay ทำงาน)**
- ใช้ advisory lock: `pg_try_advisory_lock(<const>)` ตอน start relay · ตัวที่ได้ lock เท่านั้นรัน drain
- ง่ายกว่าในแง่ logic · แต่ relay ตัวเดียว = single point (ถ้า leader ตาย ต้อง failover)

### Acceptance
- [ ] รัน 2 instance พร้อมกัน → แต่ละ event publish ครั้งเดียว (ไม่ซ้ำ)
- [ ] instance ตาย 1 ตัว → อีกตัวยัง drain ต่อได้
- [ ] `go build && go vet` ผ่าน

> ลำดับความสำคัญต่ำสุด — ตอนนี้ deploy single instance ก็ยังไม่เจอ · ทำตอนจะ scale horizontal

---

## ลำดับแนะนำ

| ลำดับ | TODO | เหตุผล | est |
|---|---|---|---|
| 1 | 🔴 Dead-letter | block ทั้งคิวได้ · อันตรายสุด · เจอได้แม้ single instance | S (~½ วัน) |
| 2 | 🟡 Janitor | table โตช้าๆ · ไม่ด่วนแต่ลืมไม่ได้ | XS (~1-2 ชม.) |
| 3 | 🟡 Multi-replica | เจอเฉพาะตอน scale horizontal | S (~½ วัน) |

## ไฟล์ที่จะแตะ (ทั้ง 3)
```
db/migrations/00XX_outbox_dead_letter.sql   ← TODO 1 (new)
db/sqlc/schema.sql                          ← sync (กฎ sqlc)
internal/adapter/postgres/outbox.go         ← OutboxRepo methods (ทั้ง 3)
internal/adapter/relay/relay.go             ← drain logic + janitor ticker (1,2,3)
cmd/api/main.go                             ← (ถ้าทำ leader election · TODO 3 ทางเลือก B)
```

## Gate (ทุก TODO)
- `go build ./... && go vet ./...` ผ่าน
- ไม่กระทบ core outbox (atomic enqueue + happy path)
- migration idempotent (`IF NOT EXISTS`) + sync schema.sql

---

---

## ฝั่ง Consumer — 2 gap เจอ+ปิดแล้ว 2026-06-13 (เดิม doc เคยเคลมว่า "ไม่มี gap")

> verify รอบ 2 จากโค้ดจริง 2026-06-13 · ดูทฤษฎี playbook §7
> ⚠️ verify รอบแรก (2026-06-12) เคลม "ไม่มี gap" — **ผิด** · เจอ 2 gap จริง (R3/R4 ล่าง) แก้แล้ว
> ⚠️ **"ไม่มี inbox table" ไม่ใช่ gap** — เป็น design choice ที่ถูกต้องสำหรับ projection (อธิบายล่าง)

### Scorecard

| concept | สถานะ | หลักฐาน |
|---|---|---|
| **Idempotent Consumer** | ✅ ทำแล้ว | `hr_projection_repo.go` → `UpsertUser`/`UpsertProject` ใช้ `ON CONFLICT (pk) DO UPDATE` |
| **Manual Commit + retry** | ✅ **แก้ R3 (2026-06-13)** | เดิม handler fail → `continue` = ข้ามตัวที่ fail (FetchMessage รอบหน้าได้ตัวถัดไป → commit เลย offset = หายถาวร) · แก้เป็น `process()` retry ตัวเดิม backoff จน success หรือครบ `handlerMaxRetries=8` → skip (กัน block) |
| **Consumer restart** | ✅ **แก้ R4 (2026-06-13)** | เดิม `Run` คืน error → goroutine ตายถาวร (broker blip = consumer ตายจน restart app) · แก้ `runTopic` เป็น restart loop + `restartBackoff=3s` (ออกเฉพาะ ctx cancel) |
| **Preventing Duplicate** | ✅ ทำแล้ว | ผ่าน UPSERT idempotency (duplicate = ทับค่าเดิม · ไม่เสียหาย) |
| **Envelope poison handling** | ✅ ทำแล้ว | envelope unmarshal พัง → log + commit + skip (poison · retry ไปก็พัง) |
| **Inbox table** (processed_events) | ⏸️ ยังไม่ทำ | **ไม่จำเป็นสำหรับ projection** (ดูเหตุผล) |

### 🔴 R3 · Manual commit เดิมทิ้ง message ตอน handler error (at-least-once พัง)
`bus/consumer.go` เดิม: `if err := h(...); err != nil { continue }` — comment เขียนว่า "FetchMessage รอบหน้าได้ตัวเดิม" แต่ **ผิด** · kafka-go `FetchMessage` คืน message ตัวถัดไปเสมอ → ตัวที่ fail ถูกข้าม · พอ commit ตัวหลัง offset เลยตัวที่ fail → restart มาก็ไม่ได้ตัวนั้นอีก = **event หายถาวร**
- **แก้:** เพิ่ม `process(ctx, env, h)` — retry ตัวเดิมด้วย exponential backoff (200ms→5s cap) จน success · block partition ชั่วคราว (รักษา order) · ครบ `handlerMaxRetries=8` (poison) → log loud + commit ข้าม · ctx cancel ระหว่าง retry → ออกโดยไม่ commit (redeliver รอบ restart)
- ⚠️ shared module → กระทบ consumer ทั้ง fleet (HR ไม่ใช้ consumer · OT ถ้ามีจะ correct ขึ้น) · build HR/inventory/kafka ผ่านหมด

### 🔴 R4 · Consumer ตายถาวรตอน FetchMessage error
`hr_sync_consumer.go` เดิม: `runTopic` เรียก `c.Run` ครั้งเดียว · Run คืน error (broker error ที่ไม่ใช่ ctx cancel) → goroutine จบ → **consumer ตายจนกว่าจะ restart app** (Kafka สะดุดชั่วคราว = projection หยุด sync ถาวร)
- **แก้:** `runTopic` เป็น restart loop · Run คืน error + ctx ยัง alive → start consumer ใหม่หลัง `restartBackoff=3s` · ออกเฉพาะตอน ctx ถูกยกเลิก (shutdown)

### หลักฐานโค้ด

**Manual commit (`erp_kafka_module/bus/consumer.go`):**
```go
m, _ := c.r.FetchMessage(ctx)            // manual · ยังไม่ commit
env, err := events.Unmarshal(m.Value)
if err != nil { c.r.CommitMessages(ctx, m); continue }  // poison envelope → skip
if err := h(ctx, env); err != nil { continue }          // fail → ไม่ commit → retry
c.r.CommitMessages(ctx, m)                               // ✅ commit หลัง process สำเร็จ
```

**Idempotent UPSERT (`internal/adapter/postgres/hr_projection_repo.go`):**
```go
INSERT INTO hr_users (...) VALUES (...)
ON CONFLICT (user_id) DO UPDATE SET name=EXCLUDED.name, ...  // ซ้ำ = ทับ · idempotent
```

### ทำไม Inbox table "ยังไม่ทำ" = ถูกต้อง (ไม่ใช่ของขาด)

| ประเภท consumer | idempotency ที่ต้องใช้ | ระบบมีไหม |
|---|---|---|
| **Projection** (sync state · cache ข้อมูล) | **UPSERT by PK** (พอแล้ว) | ✅ มี (hr_users / hr_projects) |
| **Side-effect** (ส่ง email · +/- ยอด · สร้าง record ใหม่) | **Inbox table** (เช็ค envelope.ID) | ❌ ยังไม่มี consumer แบบนี้ |

**กฎ:** UPSERT พอสำหรับ "sync ข้อมูล" · inbox จำเป็นเฉพาะ "action ที่ทำซ้ำไม่ได้"
→ ระบบตอนนี้มีแค่ projection consumer → **ถูกต้องแล้วที่ยังไม่มี inbox**

### 🔜 เมื่อไหร่ต้องทำ Inbox (TODO future)
เพิ่ม **inbox table + dedup by envelope.ID** เมื่อมี consumer ตัวแรกที่ทำ side-effect ที่ทำซ้ำไม่ได้:
- ตัวอย่าง: consume `stock.low` → ส่ง email แจ้งเตือน (ส่งซ้ำ = spam)
- ตัวอย่าง: consume `requisition.fulfilled` → +/- ตัวเลขใน report (บวกซ้ำ = ผิด)
- pattern: playbook §7.3 วิธี B + §7.4 (DB tx commit ก่อน → commit offset)

> ตอนนั้นค่อยเพิ่ม `processed_events` table + เช็ค `env.ID` ใน handler · ไม่ต้องแตะ projection consumer เดิม

---

## สรุปภาพรวม Reliability (producer + consumer)

| ส่วน | สถานะ |
|---|---|
| **Producer · core outbox** | ✅ ครบ (atomic enqueue + relay + happy path + Kafka-down) |
| **Producer · hardening** | ⚠️ เหลือ 1 gap — ✅ dead-letter (TODO1) · ✅ janitor (TODO2) · ❌ multi-replica (TODO3 · เลื่อน · ดูเหตุผล) |
| **Consumer · idempotent** | ✅ ครบ (UPSERT by PK) |
| **Consumer · at-least-once (retry)** | ✅ **แก้ R3 (2026-06-13)** — retry ตัวเดิมจน success/poison · เดิมทิ้ง message |
| **Consumer · availability (restart)** | ✅ **แก้ R4 (2026-06-13)** — restart loop · เดิมตายถาวรตอน broker error |
| **Consumer · inbox** | ⏸️ ยังไม่ต้องทำ (projection ใช้ UPSERT พอ · ทำเมื่อมี side-effect consumer) |

→ TODO 1+2 (producer) + R3+R4 (consumer) ปิดแล้ว (2026-06-13) · **gap ที่เหลือ = TODO 3 multi-replica เท่านั้น**

### TODO 3 (multi-replica) — double-publish เกิดจริง แต่ "ไม่เป็นปัญหา" (ไม่ใช่ correctness risk) · อัปเดต 2026-06-13
> ⚠️ แก้ premise เดิมที่ผิด: deploy บน **Cloud Run autoscaling (min 1 · max 10–20) + instance-based billing (CPU always-on)** ทั้ง HR และ inventory → **ไม่ใช่ single instance** · relay รันทุก instance พร้อมกันตอน scale-out → double-publish **เกิดขึ้นจริง** (HR ก็มี relay = `internal/adapter/eventpub/relay.go`)

**แต่ double-publish ไม่พัง correctness** — เพราะ 2 property ประกอบกัน:
1. **consumer idempotent** (UPSERT by PK) → event ซ้ำ = เขียนทับค่าเดิม ไม่เสียหาย
2. **relay ปัจจุบันไม่มี SKIP LOCKED** → ทุก relay ดึง row **ชุดเดียวกัน** (`WHERE published_at IS NULL ORDER BY created_at`) → publish event ตัวเดียวกันเรียงเหมือนกัน → ได้แค่ **duplicate ไม่ reorder** (event เก่ามาก่อนใหม่เสมอ ทุก relay) · `MarkPublished` race = last-write-wins ไม่เสียหาย

→ ต้นทุนเดียวที่เหลือ = **Kafka write volume × N** ตอน scale-out (cost · ไม่ใช่ correctness)

### 🔑 ห้าม "แก้" R6 ด้วย SKIP LOCKED (จะทำให้แตก)
- `SELECT FOR UPDATE SKIP LOCKED` (doc เดิมเคยแนะนำ) → relay คนละ batch · **ทำลาย per-key order ข้าม replica** (replica B publish event ใหม่กว่า ก่อน replica A publish event เก่ากว่าของ key เดียวกัน) — โค้ด naive ปัจจุบัน **ปลอดภัยกว่า** fix นี้ภายใต้ multi-replica
- ถ้าวันหนึ่งอยากตัด cost duplicate จริง → ใช้ **leader election (advisory lock)** เท่านั้น · relay เดียว active · รักษา order · **แต่** ต้องถือ dedicated conn + กัน pgxpool reap idle conn (lock หลุดเงียบ → double-publish กลับมา) — งานที่ต้องระวัง ไม่ใช่แค่ใส่ SQL
- **สรุป: ปล่อยไว้แบบนี้ดีที่สุด** · double-publish ปัจจุบัน harmless · "fix" ผิดวิธีอันตรายกว่าไม่ fix
