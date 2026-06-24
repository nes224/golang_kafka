# golang_grpc — HR service ผ่าน gRPC (Engineer / Warehouse / Procurement → HR)

Demo gRPC ตาม scenario จริงของ ERP: **3 service ยิงเข้าไปถาม HR** ว่า session ยัง active ไหม
+ ขอ roles/permissions สด แล้วดึงข้อมูลพนักงาน — แทนการเรียกผ่าน REST/HTTP

> สรุป 1 บรรทัด: เขียน contract ที่ `proto/hrpb/hr.proto` ครั้งเดียว → gen code ได้ทั้ง server + client →
> เรียก RPC ข้ามเครื่องเหมือนเรียกฟังก์ชัน local โดยมี type-safe ครบ

---

## ภาพรวม

```
   ENGINEER ───┐
   WAREHOUSE ──┼──► (gRPC :50051) ──► HR service ──► mock DB (employees/sessions)
   PROCUREMENT ┘        │
                        ├─ ValidateSession(session_id) → {active, roles, permissions}
                        └─ GetEmployee(employee_id)     → {name, department, ...}
```

- **HR** = gRPC **server** (`cmd/hr-server`) — เจ้าของข้อมูลพนักงาน/session
- **อีก 3 service** = gRPC **client** (`cmd/client` ตัวเดียว เลือก caller ด้วย flag) — ยิงเข้ามาถาม

ตรงกับ `HR_WAREHOUSE_COMMUNICATION.md` ช่องทางที่ 1 (Auth/Session — sync) ที่เดิมเป็น HTTP
ในเดโมนี้เปลี่ยนเป็น gRPC ให้ดูว่าต่างกันยังไง

---

## โครงสร้าง

```
proto/hrpb/hr.proto       ★ contract — นิยาม service + message (เขียนเอง)
proto/hrpb/hr.pb.go       generated (message) ← gen จาก .proto
proto/hrpb/hr_grpc.pb.go  generated (server/client stub) ← gen จาก .proto
internal/hr/service.go    implement HRService (mock data) ← เขียนเอง
cmd/hr-server/main.go     gRPC server :50051 ← เขียนเอง
cmd/client/main.go        client แทน 3 service ← เขียนเอง
Makefile                  install-tools / gen / server / client
```

หัวใจคือ `.proto` — ไฟล์ `*.pb.go` ทั้ง 2 ตัว **ไม่ต้องเขียนเอง** สั่ง `make gen` แล้วได้มาฟรี

---

## วิธีรัน

ต้องมี: Go 1.23+, `protoc` (macOS: `brew install protobuf`)

```bash
# 1) ติดตั้ง plugin gen Go + gen code จาก .proto
make install-tools
make gen          # ได้ proto/hrpb/hr.pb.go + hr_grpc.pb.go
make tidy         # go mod tidy

# 2) เปิด HR server (terminal 1)
make server       # [HR] gRPC server listening on :50051

# 3) ยิงจาก service ต่างๆ (terminal 2)
go run ./cmd/client --caller ENGINEER    --session sess-eng-001
go run ./cmd/client --caller WAREHOUSE   --session sess-wh-002
go run ./cmd/client --caller PROCUREMENT --session sess-proc-003
go run ./cmd/client --caller WAREHOUSE   --session sess-bad-999   # session ปลอม → ถูกปฏิเสธ
```

ผลที่ควรเห็น (เคส valid):
```
[ENGINEER] ✅ session OK · emp=E001 · roles=[engineer admin] · perms=[project.read ...]
[ENGINEER] 👤 เมฆ วิชยสาตร์ · Engineering · Lead Engineer
```

---

## gRPC ทำงานยังไง (อ่านโค้ดตามนี้)

1. **เขียน contract** (`hr.proto`) — นิยาม `service HRService` + `rpc ValidateSession(...)` + message
2. **gen code** (`make gen`) — protoc สร้าง:
   - `hr.pb.go` — struct ของ message (`ValidateSessionRequest`, `Employee`, ...) + การ encode/decode binary
   - `hr_grpc.pb.go` — `HRServiceServer` interface (ฝั่ง implement) + `HRServiceClient` (ฝั่งเรียก)
3. **server** implement interface → `RegisterHRServiceServer` → `Serve`
4. **client** `NewHRServiceClient(conn)` → เรียก `client.ValidateSession(ctx, req)` **เหมือนฟังก์ชันธรรมดา** แต่จริงๆ วิ่งข้ามเครื่อง

จุดที่ใช้ความรู้ที่เพิ่งเรียน: client ใส่ `context.WithTimeout(...)` → ถ้า HR ช้าเกิน 3 วิ ยกเลิกเอง

---

## ทำไม gRPC (เทียบ REST ที่ ERP ใช้อยู่)

| | gRPC (เดโมนี้) | REST + JSON (ของเดิม) |
|---|---|---|
| contract | `.proto` บังคับ type ชัด | เขียน doc เอง / OpenAPI |
| ข้อมูลบนสาย | binary (เล็ก/เร็ว) | JSON text |
| เรียกใช้ | `client.ValidateSession(ctx, req)` เหมือน local | ประกอบ URL + parse JSON เอง |
| type safety | compile-time (ผิด = build ไม่ผ่าน) | runtime (พังตอนรัน) |
| error | status code มาตรฐาน (`codes.NotFound`) | HTTP status + body เอง |

เหมาะกับ internal service↔service ที่เรียกถี่ + อยากได้ type-safe ข้ามภาษา
(ดูเหตุผล + เมื่อไหร่ REST ดีกว่า ในบทสนทนา/`LEARNING_LOG.md`)

---

## ⚠️ หมายเหตุ

- **ยังไม่ได้ verify compile** — sandbox ที่สร้างโค้ดไม่มี Go + protoc · ต้องรัน `make install-tools && make gen && make tidy && go build ./...` ที่เครื่องตัวเอง ถ้า error ส่งมาแก้ได้
- ต้อง `make gen` **ก่อน** ถึงจะ build ได้ เพราะ server/client import `proto/hrpb` ที่ยังไม่มีจน gen
- pin `grpc v1.67.1` + `protobuf v1.35.2` (ชุดเดียวกับ golang_pubsub) — ใช้ `grpc.NewClient` (API ใหม่ แทน `grpc.Dial`)
- เป็น learning demo · ยังไม่ production: ไม่มี TLS (ใช้ insecure), mock data ใน memory, ไม่มี auth interceptor — ของจริงต้องเพิ่ม mTLS + interceptor (logging/auth) + retry
