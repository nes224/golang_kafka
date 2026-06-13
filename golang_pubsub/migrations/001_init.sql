-- ตาราง business (เหมือนฝั่ง Kafka)
CREATE TABLE IF NOT EXISTS events (
    event_id   TEXT PRIMARY KEY,
    event_type TEXT,
    timestamp  TIMESTAMPTZ
);

-- ตาราง inbox — สมุดเช็คชื่อ event ที่เคย process (Inbox Pattern · INBOX_PATTERN.md §5.1)
CREATE TABLE IF NOT EXISTS inbox (
    event_id     TEXT PRIMARY KEY,            -- business key สำหรับ dedup
    consumer     TEXT NOT NULL,               -- เผื่อหลาย subscription ใช้ตารางร่วม
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- เผื่อ cleanup row เก่า (inbox โตเรื่อยๆ → ต้องมี retention/janitor)
CREATE INDEX IF NOT EXISTS idx_inbox_processed_at ON inbox (processed_at);
