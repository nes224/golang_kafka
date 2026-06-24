module github.com/golang_grpc

go 1.23

require (
	google.golang.org/grpc v1.67.1
	google.golang.org/protobuf v1.35.2
)

// indirect deps จะถูกเติมเมื่อรัน `go mod tidy` (หลัง gen code จาก .proto แล้ว)
