# MinIO Replication Only

Đây là một giải pháp chỉ tập trung vào replication:

MinIO1 -> Python replicator -> MinIO2

Stack này không còn dùng Kafka hay Flink. MinIO1 gửi webhook đến một service Python, service có thể quét bucket hiện có khi khởi động, dedup công việc bằng SQLite, rồi stream object từ MinIO1 sang MinIO2.

## Kiến trúc

Runtime được tách thành 3 container chạy độc lập:

- MinIO1, truy cập tại `http://localhost:9000` và `http://localhost:9001`
- MinIO2, truy cập tại `http://localhost:9002` và `http://localhost:9003`
- Replicator, truy cập tại `http://localhost:8080`

Mỗi container cùng join vào một Docker network external: `stream-s3-replication`.

## Cấu Trúc Dự Án

Các file ở mức gốc:

- `docker-compose.minio1.yml`: stack riêng cho MinIO1
- `docker-compose.minio2.yml`: stack riêng cho MinIO2
- `docker-compose.replicator.yml`: stack cho replicator và bootstrap
- `start.sh`: lệnh khởi động một phát cho Linux/macOS shell
- `start.ps1`: lệnh khởi động một phát cho PowerShell trên Windows
- `.env.example`: các biến môi trường dùng cho replicator và bootstrap script

Tài nguyên Docker:

- `docker/Dockerfile.replication`: image cho Python replicator
- `docker/bootstrap-replication.sh`: script bootstrap tạo bucket, đăng ký webhook và tạo user UI

Mã nguồn:

- `src/common`: code helper dùng chung

  - `events.py`: model event và parse JSON
  - `minio_io.py`: helper làm việc với MinIO client
  - `config.py`: module config dùng chung còn giữ lại trong repo

- `src/replication`: runtime đang được dùng

  - `app.py`: webhook server, queue, worker loop, initial sync, copy flow
  - `config.py`: settings của replicator đọc từ biến môi trường
  - `dedup.py`: nơi lưu dedup/state bằng SQLite

## Cách Chạy

Khuyến nghị:

```bash
./start.sh
```

Trên Windows PowerShell:

```powershell
.\start.ps1
```

Hai script khởi động này làm 2 việc:

1. Tạo network external `stream-s3-replication` nếu chưa tồn tại.
2. Chạy 3 compose file theo thứ tự: MinIO1, MinIO2, rồi stack replicator.

Nếu muốn chạy thủ công, dùng các lệnh sau:

```bash
docker network create stream-s3-replication
docker compose -f docker-compose.minio1.yml up -d
docker compose -f docker-compose.minio2.yml up -d
docker compose -f docker-compose.replicator.yml up --build -d
```

## Bootstrap Script Làm Gì

Khi `docker-compose.replicator.yml` chạy, nó sẽ khởi động một container `minio/mc` để thực thi `docker/bootstrap-replication.sh`.

Script này sẽ:

- đợi đến khi MinIO1 và MinIO2 sẵn sàng
- tạo bucket `images` trên cả hai MinIO nếu chưa có
- đăng ký webhook notification trên MinIO1
- tạo một user UI để upload object
- gắn policy `readwrite` cho user đó trên cả hai MinIO

## Luồng Hoạt Động

Luồng code chính nằm ở [src/replication/app.py](src/replication/app.py).

### 1. Khởi động service

`main()` đọc settings từ environment, tạo queue dùng chung, khởi động worker threads, và nếu cần thì chạy initial sync thread.

### 2. Initial sync

Nếu `INITIAL_SYNC=true`, `_initial_sync()` sẽ list object trong bucket nguồn và đẩy các event giả vào cùng queue dùng cho webhook.

Nghĩa là dữ liệu cũ và dữ liệu realtime đi chung một downstream path.

### 3. Nhận webhook

MinIO1 gửi HTTP `POST` tới `/minio-webhook` mỗi khi có object được tạo.

`WebhookHandler.do_POST()` đọc body request, kiểm tra đúng path, rồi đẩy payload vào queue.

### 4. Worker xử lý

`_worker_loop()` liên tục lấy event từ queue và gọi `_replicate_event()`.

Queue có giới hạn độ lớn, nên nếu service bị quá tải thì request sẽ bị back-pressure thay vì phình bộ nhớ vô hạn.

### 5. Quyết định copy

`_replicate_event()` parse payload thành một `StreamEvent`, rồi thực hiện các bước sau:

- bỏ qua event không thể copy
- bỏ qua event không thuộc bucket nguồn đã cấu hình
- tạo dedup key
- claim dedup key trong SQLite
- kiểm tra xem MinIO2 đã có cùng object chưa

### 6. Truyền dữ liệu

Nếu object cần được copy, replicator sẽ:

- đọc metadata từ MinIO1 bằng `stat_object`
- tải body object từ MinIO1 bằng `get_object`
- upload object sang MinIO2 bằng `put_object`

Đây là kiểu stream transfer. Toàn bộ object không bị nạp một lần vào bộ nhớ.

### 7. Hoàn tất

Sau khi truyền thành công, record sẽ được đánh dấu `done` trong SQLite.

Nếu có exception, record sẽ được release để lần retry sau có thể thử lại.

## Chiến Lược Dedup

Dedup được implement trong [src/replication/dedup.py](src/replication/dedup.py).

Dedup key được tạo từ:

- bucket
- object key
- etag

Điều này có nghĩa là:

- cùng một version của object chỉ xử lý một lần
- webhook duplicate sẽ bị bỏ qua
- event initial sync và event realtime của cùng một object version sẽ hợp nhất thành một lần copy
- restart service sẽ không xử lý lại object đã hoàn thành

### Máy Trạng Thái Dedup

Bảng SQLite lưu một dòng cho mỗi object version.

Các trạng thái:

- `processing`: record đã được claim nhưng copy chưa xong
- `done`: object đã replicate thành công hoặc đã up-to-date ở MinIO2

TTL xử lý:

- `DEDUP_PROCESSING_TTL_SECONDS` mặc định là `600`
- nếu record nằm ở `processing` quá lâu thì nó sẽ được phép xử lý lại
- điều này bảo vệ khi worker crash hoặc container dừng đột ngột

### Claim / Complete / Release

- `claim()` chèn dedup key với trạng thái `processing`
- nếu key đã tồn tại, event sẽ được coi là duplicate và bỏ qua
- `complete()` đổi trạng thái thành `done`
- `release()` xóa row nếu có lỗi trước khi hoàn tất

## Lưu State Ở Đâu

State bền vững được lưu trong SQLite.

Đường dẫn mặc định:

- `DEDUP_DB_PATH=/data/dedup.sqlite3`

Path này được mount vào Docker volume `replication-data` trong `docker-compose.replicator.yml`, nên dedup state vẫn còn sau khi restart.

Cơ sở dữ liệu lưu các trường sau:

- dedup key
- source bucket và object key
- etag
- dung lượng file
- event type
- status
- thời điểm claim
- thời điểm complete

## Biến Môi Trường

Các biến quan trọng trong `.env.example`:

- `SOURCE_MINIO_ENDPOINT`
- `SOURCE_MINIO_ACCESS_KEY`
- `SOURCE_MINIO_SECRET_KEY`
- `SOURCE_MINIO_SECURE`
- `DEST_MINIO_ENDPOINT`
- `DEST_MINIO_ACCESS_KEY`
- `DEST_MINIO_SECRET_KEY`
- `DEST_MINIO_SECURE`
- `SOURCE_BUCKET`
- `DEST_BUCKET`
- `INITIAL_PREFIX`
- `INITIAL_SYNC`
- `REPLICATION_WEBHOOK_HOST`
- `REPLICATION_WEBHOOK_PORT`
- `REPLICATION_WORKERS`
- `REPLICATION_QUEUE_SIZE`
- `DEDUP_DB_PATH`
- `DEDUP_PROCESSING_TTL_SECONDS`
- `MINIO_UI_USER`
- `MINIO_UI_PASSWORD`

## Endpoint Hữu Ích

- MinIO1 console: `http://localhost:9001`
- MinIO2 console: `http://localhost:9003`
- Replicator webhook: `http://localhost:8080/minio-webhook`

## Ghi Chú Thiết Kế

Implementation này cố ý giữ nhỏ và thực dụng.

Phù hợp với:

- đồng bộ MinIO-to-MinIO đơn giản
- write load mức vừa
- cần debug và triển khai nhanh

Tradeoff:

- replicator chạy đơn process
- SQLite nằm local trong volume của container
- không có distributed queue
- không có coordination ngang hàng giữa nhiều replicator replica

Nếu bạn cần worker replication đa node hoặc delivery guarantee mạnh hơn, hãy đưa dedup/state sang một external store như Redis hoặc Postgres.
