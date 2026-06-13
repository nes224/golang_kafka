# HR ↔ Warehouse — Communication Architecture (สำคัญ · canonical)

> สร้าง: 2026-06-12 · status: **architecture reference (single source of truth)**
> scope: ทุกการสื่อสารระหว่าง `erp_hr_module_be` (HR) ↔ `erp_inventory_module_be` (Warehouse)
> related:
> - `REFACTOR_PLAN_sqlc_hr_integration.md` (เหตุผลเลือก read-projection)
> - `erp_hr_module_be/docs/WAREHOUSE_INTEGRATION.md` (สิ่งที่ HR ต้องทำ)
> - `erp_kafka_module/topics.go` + `payloads.go` (event contract)
> - `erp_kafka_module/CASE_STUDY_inventory_hr_projection.md`

## 🎯 TL;DR — ไม่ใช่ Kafka อย่างเดียว · มี 2 ช่องทาง คนละหน้าที่

| ช่องทาง | กลไก | ใช้ทำอะไร | sync/async |
|---|---|---|---|
| **1. Auth/Session** | HTTP (REST) | ตรวจ session ยัง active + ดึง roles/perms สด | **synchronous** |
| **2. Data Sync** | Kafka | sync users/projects + publish stock/requisition events | **asynchronous** |

> ⚠️ **ห้ามเข้าใจผิดว่าทุกอย่างผ่าน Kafka** — auth ต้อง realtime → ใช้ HTTP · data ยอม eventual → ใช้ Kafka

---

## 1. ช่องทางที่ 1 · Auth/Session = HTTP synchronous

### Flow
```
Warehouse                                    HR
─────────                                    ──
ทุก request เข้า /api/inventory/*
  │
  ├─ sessioncheck.HRValidator.Valid(ctx, sessionID)
  │     │
  │     └──GET {HR_INTERNAL_URL}/api/auth/session/validate──►
  │                                                   ตรวจ user_session
  │     ◄──{ ok, user_id, email, roles, perms, session_id }──
  │
  └─ ถ้า ok → inject roles/perms เข้า gin context → perm gate
     ถ้าไม่ ok → 401
```

### รายละเอียด (จากโค้ดจริง)
- **ไฟล์:** `internal/adapter/sessioncheck/validator.go`
- **endpoint ที่เรียก:** `GET {HR}/api/auth/session/validate`
- **HR_INTERNAL_URL:** `http://localhost:8001` (default · `internal/config/config.go`)
- **wired:** `cmd/api/main.go:58` → `sessioncheck.NewHRValidator(cfg.HRInternalURL)`
- **posTTL = 0** → เช็คสดทุก request (ไม่ cache · strong consistency)
- HR response: `{ ok, user_id, email, roles, perms, session_id }`

### ทำไม HTTP ไม่ใช่ Kafka
- ต้อง **realtime + strong consistency** — user โดน revoke/logout ต้อง 401 **ทันที**
- Kafka มี lag (eventual) → ทำ auth gate ไม่ได้ · จะมีช่วง user ที่ถูกแบนยังเข้าได้
- JWT อย่างเดียวไม่พอ (revoke ไม่ได้ก่อน exp) → ต้อง sessioncheck สด

### ✅ สถานะ: **wired + ทำงานแล้ว**

---

## 2. ช่องทางที่ 2 · Data Sync = Kafka asynchronous

มี 2 ทิศทาง:

### 2A · HR → Warehouse (consume · projection)

```
HR  ──publish──►  Kafka topic         Warehouse consumer            local projection
                  ───────────         ──────────────────            ────────────────
                  hr.users      ────► inventory-hr-users-sync   ──► hr_users
                  hr.projects   ────► inventory-hr-projects-sync ──► hr_projects
```

**Contract (`erp_kafka_module/topics.go` + `payloads.go`):**

| topic | event types | payload | warehouse ทำอะไร |
|---|---|---|---|
| `hr.users` | `user.created` `user.updated` `user.deactivated` | `UserPayload{ user_id int64, email, name, roles[], is_active }` | upsert `hr_users` projection |
| `hr.projects` | `project.created` `project.updated` `project.closed` | `ProjectPayload{ project_id int64, code, name, owner, province, project_cost, active }` | upsert `hr_projects` projection |

**โค้ด warehouse (consumer):**
- ไฟล์: `internal/adapter/consumer/hr_sync_consumer.go`
- wired: `cmd/api/main.go:93` → `consumer.NewHRSync(...)` + `go hrSync.Run(relayCtx)`
- consumer group: `inventory-hr-users-sync`, `inventory-hr-projects-sync`
- projection repo: `internal/adapter/postgres/hr_projection_repo.go` (read-only cache · เขียนจาก consumer เท่านั้น)

**สถานะ:**
- ✅ **Warehouse consumer: wired + start แล้ว** (subscribe รออยู่)
- ❌ **HR producer: ยังไม่มี** → consumer นั่งรอเฉยๆ ไม่มี event มา

### 2B · Warehouse → ผู้บริโภคอื่น (produce · outbox)

```
Warehouse business write (tx)              outbox relay              Kafka
─────────────────────────────              ────────────              ─────
CreateReceipt/Ship/Receive/...
  │ INSERT business + enqueueEvent  ─┐
  │ (atomic · tx เดียวกัน)          │
  └─ event_outbox table  ───────────┘
                                      relay goroutine (poll 2s)
                                      ──pick unpublished──► publish ──► inventory.stock
                                                                        inventory.requisitions
                                      ──mark published──
```

**Contract:**

| topic | event types | trigger ใน warehouse |
|---|---|---|
| `inventory.stock` | `stock.received` `stock.issued` `stock.returned` (`stock.adjusted` `stock.low` ยังไม่ publish) | CreateReceipt · Ship (ISSUE) · CreateReturn |
| `inventory.requisitions` | `requisition.created` `requisition.fulfilled` | Create · Receive (DONE) |

**โค้ด warehouse (producer):**
- enqueue: `internal/adapter/postgres/outbox.go` → `enqueueEvent(ctx, tx, ...)` (ใน business tx)
- relay: `internal/adapter/relay/relay.go` → poll outbox → publish → mark
- wired: `cmd/api/main.go:88` → `relay.New(...)` + `go rl.Run(relayCtx)`
- pattern: **transactional outbox** (at-least-once · กัน dual-write)

**สถานะ:**
- ✅ **wired + ทำงานแล้ว** (event ออกได้จริง · ถ้ามี broker)

---

## 3. Envelope contract (ทุก event ใช้ร่วมกัน)

`erp_kafka_module/envelope.go`:
```jsonc
{
  "id": "<uuid>",              // idempotency key (consumer เช็คกันซ้ำ)
  "type": "user.updated",      // event type
  "version": 1,                // schema version
  "source": "hr",              // hr | ot | inventory
  "correlation_id": "...",     // saga flow id (optional)
  "causation_id": "...",       // event ที่ทำให้เกิด event นี้ (optional)
  "payload": { ... }           // UserPayload | ProjectPayload | StockPayload | ...
}
```

**กฎ:**
- consumer ต้อง **idempotent** (เช็ค `envelope.id` กันซ้ำ · UPSERT by PK)
- message **key = aggregate id** (รักษา order ต่อ entity ใน partition)
- เพิ่ม field = ปลอดภัย · breaking change = **bump `version`** + plan migration

---

## 4. สรุปสถานะปัจจุบัน (matrix)

| flow | ทิศทาง | กลไก | code wired? | ทำงานจริง? | บล็อกที่ |
|---|---|---|---|---|---|
| sessioncheck | WH → HR | HTTP | ✅ | ✅ | — |
| outbox relay (produce) | WH → Kafka | Kafka | ✅ | ✅ | — |
| hr-sync consumer | Kafka → WH | Kafka | ✅ | ⚠️ รอ event | **HR ยังไม่ produce** |
| **HR producer hr.users** | HR → Kafka | Kafka | ❌ | ❌ | **ต้องทำที่ HR** |
| **HR producer hr.projects** | HR → Kafka | Kafka | ❌ | ❌ | **ต้องทำที่ HR** |
| HR snapshot (backfill) | WH → HR | HTTP | ❌ | ❌ | endpoint ยังไม่มีที่ HR |

> **Warehouse ฝั่งพร้อมหมดแล้ว** · gap ทั้งหมดอยู่ฝั่ง HR (ดู `erp_hr_module_be/docs/WAREHOUSE_INTEGRATION.md` ข้อ 2-5)

---

## 5. ⚠️ Known gap — ID alignment (int64 vs text)

**ปัญหาที่ต้องแก้ก่อน wire projection จริง:**

| ที่ | PK type |
|---|---|
| HR users/projects (source) | `int64` (ดู `UserPayload.user_id int64`, `ProjectPayload.project_id int64`) |
| Warehouse local users/projects | `text` (cuid/uuid) |

→ ตอน Phase 3 (สลับ join จาก local → projection) ต้อง:
- align ID type · เลือก int64 เป็นมาตรฐาน (breaking → bump version + migration)
- หรือ map ผ่าน email/code (natural key) แทน int id

> รายละเอียด: `HANDOFF_sqlc_phase1.md` (ID alignment เป็น int64) + `REFACTOR_PLAN_sqlc_hr_integration.md` Phase 3

---

## 6. ⚠️ Known gap — naming inconsistency (ต้องเคาะ)

`CASE_STUDY_inventory_hr_projection.md` flag ไว้:
- `topics.go` (implement จริง) ใช้ `hr.users` + `user.*`
- architecture doc บางที่อ้าง `hr.employee.v1` + `employee.*`

→ **ยึด `topics.go` = `hr.users` + `user.*`** (ที่ consumer warehouse subscribe อยู่จริง)
→ HR producer ต้องใช้ topic/type ตาม `topics.go` ไม่ใช่ตาม architecture doc เก่า

---

## 7. Diagram รวม (ภาพเดียวจบ)

```
                    ┌──────────────────────── HR (erp_hr_module_be · :8001) ──────────┐
                    │  users · projects (source of truth)                              │
                    │  session store                                                   │
                    └──┬──────────────────────────────┬───────────────────────────────┘
                       │ ①HTTP sync                    │ ②Kafka async (ยังไม่ทำ)
                       │ GET /api/auth/                │ produce hr.users
                       │     session/validate          │         hr.projects
                       │ (ทุก request · realtime)       │ (master-data sync · eventual)
                       ▼                                ▼
        ┌──────────────────────────── Warehouse (erp_inventory_module_be · :8003) ─────┐
        │  sessioncheck.Validator ◄─①   consumer.HRSync ◄─② → hr_users / hr_projects   │
        │                                                       (projection · read-only)│
        │                                                                                │
        │  business write (tx) → event_outbox → relay → ③produce ──►                     │
        └────────────────────────────────────────────────────────────┬─────────────────┘
                                                                       │ ③Kafka async (ทำแล้ว)
                                                                       ▼
                                          inventory.stock · inventory.requisitions
                                          (ผู้บริโภคอื่น: HR? reporting? procurement?)
```

---

## 8. Checklist ทำให้ครบ loop (เรียงลำดับ)

- [x] WH sessioncheck (HTTP) — ✅ ใช้ได้
- [x] WH outbox relay (produce) — ✅ ใช้ได้
- [x] WH hr-sync consumer — ✅ wired (รอ event)
- [ ] **HR producer `hr.users`** (outbox pattern · ตาม `topics.go`) — 🔴 P0
- [ ] **HR producer `hr.projects`** — 🔴 P0
- [ ] **HR snapshot endpoint** (`/internal/users/snapshot`, `/internal/projects/snapshot`) สำหรับ backfill — 🟡 P1
- [ ] **เคาะ ID alignment** (int64 vs text) ก่อน Phase 3 — 🔴 P0
- [ ] **เคาะ naming** (`hr.users`+`user.*` ยึด topics.go) — 🟡 P1

> ทั้งหมดฝั่ง HR · ดู `erp_hr_module_be/docs/WAREHOUSE_INTEGRATION.md` (action items)
