package repo

import (
	"fmt"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // ลงทะเบียน driver "postgres"
)

// dsn อ่านค่าจาก env ทั้งหมด — แก้ Known Issue ของฝั่ง Kafka (password hardcode ใน db.go)
// default ตั้งไว้ให้รัน local ได้เลย · production ให้ override ผ่าน env / secret manager
func dsn() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		env("DB_HOST", "localhost"),
		env("DB_PORT", "5433"),
		env("DB_USER", "alphamech"),
		env("DB_PASSWORD", "alphamech1234@"),
		env("DB_NAME", "kafka_yt"),
	)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func NewDBConn() (*sqlx.DB, error) {
	return sqlx.Connect("postgres", dsn())
}
