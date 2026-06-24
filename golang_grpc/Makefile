.PHONY: install-tools gen tidy server client

# 1) ติดตั้ง plugin ที่ protoc ต้องใช้ gen Go (รันครั้งเดียว)
#    ต้องมี protoc ในเครื่องด้วย (macOS: brew install protobuf)
install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 2) gen code จาก .proto → ได้ proto/hrpb/hr.pb.go + hr_grpc.pb.go
gen:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/hrpb/hr.proto

# 3) เติม dependency หลัง gen
tidy:
	go mod tidy

# รัน HR server
server:
	go run ./cmd/hr-server

# รัน client — ปรับ caller/session ได้
client:
	go run ./cmd/client --caller WAREHOUSE --session sess-wh-002
