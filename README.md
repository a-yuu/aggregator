# UTS LOG AGGREGATOR
## Ringkasan
- Aplikasi log aggregator berbasis FastAPI dengan pemrosesan asinkron, 5 worker paralel, dan dedup store menggunakan SQLite.
- Sistem menerapkan model at-least-once + idempotency untuk memastikan setiap event unik hanya diproses sekali, walaupun terjadi duplikasi.
- Deploy dilakukan melalui Docker Compose.
## Arsitektur Singkat
Client → FastAPI (validasi) → Async Queue → Event Aggregator (5 worker) → Dedup Store (SQLite, persisten via volume)
## Syarat Implementasi
- Docker File
- Docker Compose
- python:3.11-slim
## Build & Run
### Build Image
`` `json 
docker build -t log-aggregator .
` ``
### Build Docker Compose
`` `json 
docker-compose up –build
` ``

Compose akan:
- Membangun dan menjalankan service aggregator dan publisher
- Membuat network internal dan volume untuk SQLite
- Menjalankan FastAPI pada container port 8080, dapat diakses di http://localhost:8080

### Menghentikan compose untuk direstart
`` `json 
docker compose down
` ``

Volume tidak dihapus, sehingga dedup store tetap tersimpan dan event lama tetap dikenali sebagai duplikat. Setelah itu, jalankan kembali

`` `json 
docker-compose up –build
` ``

## Penggunaan Desain
- At-least-once delivery: Duplikasi bisa terjadi saat retry.
- Idempotent consumer + dedup store (berdasarkan event_id): Mencegah pemrosesan ganda.
- SQLite: Ringan, ACID-compliant, tanpa setup server, cukup untuk skala UTS, dan mudah dibackup.
- Lima worker: Memberi throughput terbaik tanpa menimbulkan contention ke database (±190 event/detik pada pengujian lokal).
- Eventual consistency: Sistem akan konvergen, event unik tersimpan hanya sekali.

## Endpoint
### POST /Publish
Mengirim code dibawah ini:

`` `json 
curl -X POST 'http://localhost:8080/publish' \
  -H 'accept: application/json' -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {
        "event_id": "evt_1",
        "payload": {"email": "ayu@example.com", "user_id": 1},
        "source": "auth-service",
        "timestamp": "2025-10-19T10:30:00Z",
        "topic": "user.created"
      }
    ]
  }'
` ``

respon:

`` `json 
{
  "status": "success",
  "message": "Processed 1 events",
  "details": {"accepted": 1, "rejected": 0, "duplicates_immediate": 0}
}
` ``
rejected dan duplicated_immediate akan akan berubah dari 0 menjadi 1 atau 2 tergantung jumlah yang terdeteksi.

### GET /events
Ambil daftar event tersimpan.

Parameter opsional:
- topic → filter berdasarkan topik
- limit (default 100)
- offset (default 0)

`` `json 
curl -X GET 'http://localhost:8080/events?topic=user.created&limit=50&offset=0' \
  -H 'accept: application/json'
` ``

respons:

`` `json 
{"status":"success","count":2,"events":[{...}, {...}]}
` ``

### GET /stats
GET /stats digunakan untuk melihat statistik runtime sistem.

`` `json 
curl -X GET 'http://localhost:8080/stats' -H 'accept: application/json'
` ``

respon:
`` `json 
{
  "received": 4,
  "unique_processed": 2,
  "duplicate_dropped": 2,
  "topics": {"user.created": 2},
  "uptime": 2250.48
}
` ``

Setelah restart tanpa menghapus volume, nilai duplicate_dropped akan meningkat jika event lama dikirim ulang.
Hal ini menunjukkan deduplication dan idempotensi berjalan sesuai harapan.

### GET /health
GET /health digunakan untuk mengecek status kesehatan service

`` `json 
curl -X GET 'http://localhost:8080/health'
` ``

## Unit Test Lokal

`` `json 
python -m pytest -q -p no:warning
` ``

contoh respon:

`` `json 
9 passed in 14.44s
` ``

## NOTE!!!
- Idempotency: Event dengan event_id sama hanya diproses sekali.
- Dedup store persisten: Data disimpan di SQLite melalui volume Docker, aman terhadap restart.
- Throughput tinggi: 5 worker paralel meningkatkan kapasitas pemrosesan.
- Jika port konflik gunakan port lain seperti 8081:8080

Link YouTube: https://youtu.be/H0nz-_-x8bQ?si=idi_zzIrh1ZbxAcG 

Link Laporan: https://drive.google.com/file/d/1I_YN-UcO-Os87dmM9CHzIWe2FC8qZpsc/view?usp=sharing 
