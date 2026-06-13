# CDC / Debezium — ส่งการเปลี่ยนแปลงจาก DB เข้า Kafka อัตโนมัติ

บทเรียนนี้อธิบาย **Change Data Capture (CDC)** — เทคนิคจับการเปลี่ยนแปลงในตาราง DB แล้วส่งเข้า Kafka โดยแอปไม่ต้องเขียนโค้ดยิงเอง — และ **Debezium** ตัวที่ทำงานนี้ พร้อมเทียบกับ **Outbox pattern** ที่ลงโค้ดใน ERP ไปแล้ว

> สรุป 1 บรรทัด: แทนที่แอปจะยิง event เข้า Kafka เอง (outbox + relay) ให้ Debezium อ่าน **transaction log (WAL)** ของ Postgres ตรงๆ แล้วแปลงทุก INSERT/UPDATE/DELETE เป็น event เข้า Kafka อัตโนมัติ — แอปแค่เขียน DB ตามปกติ ไม่ต้องรู้จัก Kafka เลย

> อ่านคู่กับ: `INBOX_PATTERN.md` (กันซ้ำขาเข้า), `HR_KAFKA_WIRING.md` (Outbox จริงใน ERP), `LEARNING_ROADMAP.md §1.3` (บทนี้), `KAFKA_GLOSSARY.md` (คำศัพท์)

---

## สารบัญ

1. [ปัญหาที่ CDC แก้ — ทบทวน dual-write](#1-ปัญหาที่-cdc-แก้--ทบทวน-dual-write)
2. [CDC คืออะไร + WAL ทำงานยังไง](#2-cdc-คืออะไร--wal-ทำงานยังไง)
3. [Debezium architecture](#3-debezium-architecture)
4. [Config จริง — Postgres + Debezium connector](#4-config-จริง--postgres--debezium-connector)
5. [หน้าตา event ที่ Debezium ยิงออกมา](#5-หน้าตา-event-ที่-debezium-ยิงออกมา)
6. [Outbox vs CDC — ตารางเทียบ + decision tree](#6-outbox-vs-cdc--ตารางเทียบ--decision-tree)
7. [Outbox + CDC hybrid (Outbox Event Router)](#7-outbox--cdc-hybrid-outbox-event-router)
8. [Map กลับไป ERP จริง — ถ้าจะย้าย relay → Debezium](#8-map-กลับไป-erp-จริง--ถ้าจะย้าย-relay--debezium)

---

## 1. ปัญหาที่ CDC แก้ — ทบทวน dual-write

ปัญหาเดิมที่ Outbox แก้ คือ **dual-write**: เขียน 2 ระบบ (business DB + Kafka) ให้ atomic พร้อมกันไม่ได้

```go
// แบบ naive — พังได้
tx.Commit()                  // ✅ business เขียน DB สำเร็จ
producer.Produce(event)      // ❌ ถ้า crash ตรงนี้ → DB มีข้อมูล แต่ Kafka ไม่มี event = event หาย
```

**Outbox แก้ยังไง** (วิธีที่ ERP ใช้): เขียน event ลงตาราง `outbox` ใน tx เดียวกับ business → relay (โค้ดที่เขียนเอง) poll ตาราง outbox → ยิงเข้า Kafka → mark sent

**CDC แก้ยังไง**: ไม่ต้องมีตาราง outbox ไม่ต้องเขียน relay — ให้ Debezium อ่าน **log การเปลี่ยนแปลงของ DB เอง** (ทุก commit ถูกบันทึกใน WAL อยู่แล้ว) แล้วแปลงเป็น event

ทั้งคู่แก้ปัญหาเดียวกัน (event ไม่หาย, atomic กับ business) แต่คนละวิธี — §6 จะเทียบว่าเมื่อไหร่ควรใช้อันไหน

---

## 2. CDC คืออะไร + WAL ทำงานยังไง

**Change Data Capture** = การจับ "ทุกการเปลี่ยนแปลงข้อมูล" (INSERT/UPDATE/DELETE) ในตาราง แล้ว stream ออกไประบบอื่น แบบ real-time โดยไม่กระทบ performance ของ DB หลัก

หัวใจอยู่ที่ **WAL (Write-Ahead Log)** ของ Postgres:

```
ทุก transaction ที่ commit → Postgres เขียนลง WAL ก่อน เสมอ (durability)
WAL = log ลำดับเหตุการณ์ทุกการเปลี่ยนแปลง (append-only)
ปกติ WAL ใช้สำหรับ crash recovery + replication ไปยัง replica

CDC = "แอบอ่าน" WAL ตัวเดียวกันนี้ → ได้ทุก change โดยไม่ต้อง query ตาราง
```

วิธีอ่าน WAL มี 2 แบบ:

| วิธี | กลไก | ข้อดี/ข้อเสีย |
|---|---|---|
| **Log-based** (Debezium ใช้) | อ่าน WAL ผ่าน logical replication slot | ✅ ครบทุก change, ไม่พลาด, ไม่กิน CPU query / ❌ ต้องตั้งค่า DB |
| **Query-based** (polling) | `SELECT WHERE updated_at > last` ทุก N วิ | ✅ ง่าย / ❌ พลาด DELETE, พลาด change ระหว่าง poll, กิน DB |

Debezium ใช้ **log-based** ผ่านฟีเจอร์ **logical replication** ของ Postgres — Postgres แปลง WAL (ซึ่งเป็น binary ระดับ physical) ให้เป็น logical change stream (row-level) ผ่าน plugin ชื่อ `pgoutput` (built-in ตั้งแต่ PG 10)

**3 สิ่งที่ Postgres ต้องมีสำหรับ CDC:**

```
1. wal_level = logical          ← ให้ WAL เก็บข้อมูลพอจะ decode เป็น row-level change
2. Replication slot             ← "บุ๊กมาร์ก" ว่า Debezium อ่าน WAL ถึงไหนแล้ว (กันลบ WAL ที่ยังไม่อ่าน)
3. Publication                  ← ระบุว่าจะ capture ตารางไหนบ้าง
```

> ⚠️ ข้อควรระวัง replication slot: ถ้า Debezium ตายนานๆ slot จะค้าง → Postgres **เก็บ WAL ไม่ยอมลบ** (เพราะยังมีคนรออ่าน) → disk เต็มได้ ต้อง monitor `pg_replication_slots` และมี alert

---

## 3. Debezium architecture

Debezium ไม่ใช่โปรแกรมเดี่ยว — มันรันเป็น **connector บน Kafka Connect**:

```
┌──────────────┐   logical      ┌─────────────────────────────┐   produce   ┌─────────┐
│  PostgreSQL   │  replication   │       Kafka Connect          │  ────────▶  │  Kafka   │
│              │ ──────────────▶ │  ┌────────────────────────┐ │             │  topics  │
│  WAL + slot  │   (pgoutput)    │  │ Debezium PG Connector  │ │             │          │
│  publication │                 │  │ - อ่าน WAL             │ │             │ server1. │
└──────────────┘                 │  │ - แปลงเป็น change event│ │             │ public.  │
                                  │  │ - track offset ใน Kafka│ │             │ projects │
                                  │  └────────────────────────┘ │             └─────────┘
                                  └─────────────────────────────┘
                                   Kafka Connect จัดการ: restart, offset,
                                   scaling, schema → ไม่ต้องเขียน relay เอง
```

ส่วนประกอบ:
- **Kafka Connect** — framework รัน connector (มี REST API ลง config, จัดการ fault-tolerance, offset)
- **Debezium PostgreSQL Connector** — plugin ที่อ่าน WAL ของ PG ตัวนึง
- **Topic อัตโนมัติ** — Debezium สร้าง 1 topic ต่อ 1 ตาราง ชื่อ `<server>.<schema>.<table>` (เช่น `hapserver.public.projects`)
- **Offset/history topics** — Connect เก็บว่าอ่าน WAL ถึง LSN ไหนใน Kafka เอง (ไม่ใช่ในแอป)

**จุดต่างสำคัญจาก Outbox relay:** relay คือโค้ด Go ที่ต้อง maintain เอง (poll + produce + mark sent) ส่วน Debezium คือ infra สำเร็จรูป — ได้ fault-tolerance/scaling/exactly-once ฟรี แต่ต้องลง Kafka Connect cluster เพิ่ม

---

## 4. Config จริง — Postgres + Debezium connector

### 4.1 ตั้ง Postgres (postgresql.conf หรือ docker)

```conf
wal_level = logical          # บังคับ — ไม่งั้น CDC ทำงานไม่ได้
max_wal_senders = 4          # จำนวน process ที่ stream WAL ได้พร้อมกัน
max_replication_slots = 4    # จำนวน slot สูงสุด
```

ใน docker-compose (ต่อยอดจาก `kafka.yaml` ของโปรเจกต์):

```yaml
postgres:
  image: postgres:16
  command: ["postgres", "-c", "wal_level=logical",
            "-c", "max_wal_senders=4", "-c", "max_replication_slots=4"]
  environment:
    POSTGRES_DB: kafka_yt
    POSTGRES_USER: alphamech
    POSTGRES_PASSWORD: ${PG_PASSWORD}   # ★ อย่า hardcode (ดู known issue #1)
```

ให้ user ที่ Debezium ใช้มีสิทธิ์ replication:

```sql
ALTER ROLE alphamech WITH REPLICATION;
-- หรือสร้าง user เฉพาะ debezium ก็ได้ (แนะนำ — least privilege)
```

### 4.2 ลง connector ผ่าน Kafka Connect REST API

```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "projects-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "alphamech",
    "database.password": "${PG_PASSWORD}",
    "database.dbname": "kafka_yt",
    "topic.prefix": "hapserver",
    "plugin.name": "pgoutput",
    "table.include.list": "public.projects",
    "slot.name": "debezium_projects",
    "publication.autocreate.mode": "filtered",
    "tombstones.on.delete": "true"
  }
}'
```

ผลลัพธ์: ทุก change ในตาราง `public.projects` → topic `hapserver.public.projects` อัตโนมัติ

---

## 5. หน้าตา event ที่ Debezium ยิงออกมา

Debezium event มี structure มาตรฐาน `before` / `after` / `op` / `source` — ตัวอย่าง UPDATE:

```json
{
  "op": "u",                          // c=create, u=update, d=delete, r=snapshot(read)
  "ts_ms": 1718200000000,
  "before": { "id": 42, "name": "Old Name",  "cost": 1000000, "active": true },
  "after":  { "id": 42, "name": "New Name",  "cost": 1200000, "active": true },
  "source": {
    "db": "kafka_yt", "schema": "public", "table": "projects",
    "lsn": 23456789, "txId": 567        // ตำแหน่งใน WAL — ใช้ dedup/ordering ได้
  }
}
```

ประเด็นที่ต่างจาก outbox event ที่ออกแบบเอง:
- **ได้ทั้ง before + after** — รู้ค่าเก่า/ใหม่ ทำ audit/diff ได้ฟรี (outbox ต้องใส่เอง)
- **เป็น row-level ดิบ** — ได้ทุก column ของตาราง ไม่ใช่ business event ที่ออกแบบมา (เช่น `project.cost_increased`) → consumer ต้องตีความเอง
- **`op: d` (delete)** — CDC จับ DELETE ได้ ส่วน outbox ปกติไม่ค่อยทำ
- **snapshot (`op: r`)** — ตอน connector เริ่มครั้งแรก Debezium อ่านตารางทั้งหมดเป็น snapshot ก่อน แล้วค่อยตาม WAL ต่อ → แก้ปัญหา "backfill ครั้งแรก" ที่ `HR_KAFKA_WIRING.md` ระบุว่ายังไม่ทำ ได้อัตโนมัติ

---

## 6. Outbox vs CDC — ตารางเทียบ + decision tree

| มิติ | Outbox + relay (ที่ใช้อยู่) | CDC / Debezium |
|---|---|---|
| **แอปต้องรู้จัก Kafka?** | ใช่ (relay ยิงเอง) | ไม่ — เขียน DB อย่างเดียว |
| **โค้ดที่ต้อง maintain** | relay + outbox table + migration | config (ไม่มีโค้ด) แต่ต้อง maintain Connect cluster |
| **รูปแบบ event** | ✅ business event ที่ออกแบบเอง (`project.created`) | row-level ดิบ (ต้อง transform) |
| **infra เพิ่ม** | ไม่มี (อยู่ในแอป) | Kafka Connect cluster + replication slot |
| **DELETE / before-image** | ต้องเขียนเอง | ได้ฟรี |
| **Backfill ครั้งแรก** | เขียนเอง (`HR_KAFKA_WIRING` ยังไม่ทำ) | snapshot อัตโนมัติ |
| **ภาระ DBA** | ต่ำ | สูง (wal_level, slot monitoring, disk) |
| **เหมาะกับ** | event ที่มี business meaning ชัด, ทีมเล็ก, control เต็ม | sync หลายตาราง/หลายระบบ, ไม่อยากแตะ legacy code, data pipeline/warehouse |

**Decision tree:**

```
อยากส่งการเปลี่ยนแปลง DB เข้า Kafka
│
├─ event ต้องมี business semantic ชัดเจน (project.cost_increased ไม่ใช่ row update)?
│     └─ ใช่ → Outbox (ออกแบบ event เองได้)   ← งาน HR เป็นแบบนี้
│
├─ ต้อง sync หลายตาราง/ทั้ง schema ไปหลายระบบ โดยไม่อยากแตะ application code?
│     └─ ใช่ → CDC (ลงทีเดียวครอบทุกตาราง)
│
├─ มี legacy app ที่แก้โค้ดไม่ได้ แต่ต้องดึง data ออก?
│     └─ ใช่ → CDC (อ่าน DB ตรงๆ ไม่ต้องแตะแอป)
│
└─ ทีมเล็ก ไม่อยาก maintain Connect cluster + ไม่มี DBA ดูแล slot?
      └─ ใช่ → Outbox (infra น้อยกว่า)
```

> สำหรับ ERP ตอนนี้: **Outbox ถูกต้องแล้ว** เพราะ event มี business meaning (`project.created/updated/closed`) และทีมคุม code เต็ม CDC จะคุ้มเมื่อ (ก) ต้อง sync ตารางจำนวนมากข้ามหลาย service หรือ (ข) มี legacy module ที่แก้ไม่ได้

---

## 7. Outbox + CDC hybrid (Outbox Event Router)

ไม่ต้องเลือกข้างเดียว — มี pattern ลูกผสมที่ได้ข้อดีทั้งคู่ เรียก **Outbox Event Router** (Debezium มี SMT สำเร็จรูปชื่อ `EventRouter`):

```
แอปเขียน business + INSERT outbox (tx เดียว)   ← ได้ business event ที่ออกแบบเอง + atomic
                  │
                  ▼
       Debezium อ่าน WAL ของ "ตาราง outbox"      ← แทน relay ที่เขียนเอง
                  │
                  ▼
       EventRouter SMT route ไป topic ตาม column  ← เช่น aggregate_type = "project" → topic hr.projects
```

ได้อะไร:
- **business event ที่ออกแบบเอง** (จุดแข็งของ outbox) — เพราะอ่านจากตาราง outbox ที่แอปเขียน payload เอง
- **ไม่ต้อง maintain relay** (จุดแข็งของ CDC) — Debezium จัดการ poll/produce/offset/retry ให้

นี่คือ **upgrade path ที่เป็นธรรมชาติที่สุด**: โครง outbox มีอยู่แล้ว ถ้าวันหนึ่ง relay ที่เขียนเองเริ่มเป็นภาระ (ต้องดูแล retry, ordering, scaling) → เปลี่ยน relay เป็น Debezium EventRouter โดย**ตาราง outbox + business code ไม่ต้องแก้เลย** แค่ลบ relay ออกแล้วชี้ Debezium มาที่ตาราง outbox

---

## 8. Map กลับไป ERP จริง — ถ้าจะย้าย relay → Debezium

สถานะปัจจุบันใน `HR_KAFKA_WIRING.md`:
```
HR: business + outbox (tx เดียว) → relay (Go, poll ทุก 2 วิ) → Kafka hr.projects
inventory: consume hr.projects → upsert projection
```

ถ้าจะย้ายไป Debezium EventRouter (ไม่บังคับ — ทำเมื่อ relay เริ่มเป็นภาระ):

| ขั้น | ทำอะไร | กระทบโค้ด HR? |
|---|---|---|
| 1 | ตั้ง Postgres `wal_level=logical` + replication slot | ❌ ไม่ |
| 2 | ลง Kafka Connect + Debezium connector ชี้ตาราง `event_outbox` | ❌ ไม่ |
| 3 | ตั้ง EventRouter SMT: route ตาม `aggregate_type` → topic `hr.projects` (ให้ชื่อ topic เดิม) | ❌ ไม่ |
| 4 | ปิด/ลบ relay goroutine ใน `cmd/api/main.go` | ✅ ลบโค้ด relay |
| 5 | inventory consumer **ไม่ต้องแก้** ถ้า topic + payload format เหมือนเดิม | ❌ ไม่ |

**ข้อดีที่จะได้:** ไม่ต้อง maintain relay (retry/ordering/backpressure เป็นของ Connect), snapshot/backfill อัตโนมัติ (แก้ TODO "backfill ครั้งแรก" ใน `HR_KAFKA_WIRING.md`)

**ราคาที่ต้องจ่าย:** ต้องดูแล Kafka Connect cluster + monitor replication slot (disk), เพิ่ม component ใน infra, ทีมต้องเรียนรู้ Debezium

**คำแนะนำ:** อย่าเพิ่งย้ายตอนนี้ — relay ที่มีอยู่ทำงานได้และ control เต็ม ให้ทำ Phase 2 (security/shutdown/RF) ของ golang_kafka roadmap ให้จบก่อน CDC เป็นการ optimize ตอน scale ไม่ใช่ของที่ต้องมีตั้งแต่แรก เก็บบทนี้ไว้เป็น reference ตอนที่ relay เริ่มไม่ไหว (หลายตาราง, ordering ซับซ้อน, ต้อง audit before/after)

---

## สรุป

- **CDC** = จับทุก change ใน DB ผ่าน WAL แล้ว stream เข้า Kafka โดยแอปไม่ต้องยิงเอง
- **Debezium** = connector บน Kafka Connect อ่าน WAL ผ่าน logical replication (`pgoutput`) → topic อัตโนมัติต่อตาราง
- **Postgres ต้องมี:** `wal_level=logical` + replication slot + publication (ระวัง slot ค้าง = disk เต็ม)
- **event format:** before/after/op/source — ได้ DELETE + snapshot + audit ฟรี แต่เป็น row-level ดิบต้อง transform
- **Outbox vs CDC:** outbox = business event + control เต็ม + infra น้อย (ใช้ถูกแล้ว) / CDC = sync หลายตาราง + ไม่แตะ code + ภาระ DBA สูง
- **Hybrid (EventRouter):** Debezium อ่านตาราง outbox แทน relay — ได้ business event + ไม่ต้อง maintain relay = upgrade path ของ ERP
- **คำแนะนำ:** ยังไม่ต้องย้าย ทำ Phase 2 ให้จบก่อน เก็บ CDC ไว้ตอน relay เริ่มเป็นภาระ

> จบ Phase 1 (Inbox → Outbox → CDC) ครบทั้ง correctness patterns — ถัดไปคือ Phase 2 production hardening (`LEARNING_ROADMAP.md`)
