# Lab 28 - Full Platform Integration Sprint

Lab này dựng một AI platform hybrid: local chạy Kafka, Prefect, Delta Lake dạng file batch, Redis feature store, Qdrant, API Gateway, Prometheus và Grafana; Kaggle GPU chạy vLLM và embedding service qua ngrok.

## Kiến trúc

```text
Local Docker Compose
Kafka -> Prefect flow -> Delta Lake file batch -> Redis feature store
                     -> Qdrant vector store
Qdrant + vLLM -> FastAPI API Gateway -> Prometheus -> Grafana
LangSmith tracing dùng cho API Gateway nếu có LANGCHAIN_API_KEY

Kaggle GPU
vLLM OpenAI-compatible server
Embedding API /embed
MLflow metadata tracking
```

## Chuẩn bị

- Docker Desktop đang chạy.
- Python 3.10+.
- Kaggle Notebook có GPU và ngrok token.
- Cài Python dependencies local khi cần chạy script/test:

```bash
python -m pip install -r requirements.txt
```

## Cấu hình môi trường

Tạo file `.env` từ mẫu:

```bash
cp .env.example .env
```

Cập nhật các giá trị chính:

```env
VLLM_NGROK_URL=https://your-vllm.ngrok-free.app
EMBED_NGROK_URL=https://your-embed.ngrok-free.app
LANGCHAIN_API_KEY=your_langsmith_key
LANGCHAIN_PROJECT=lab28-platform
```

Nếu chưa có Kaggle URL, stack vẫn start được nhờ fallback local; để demo/nộp bài thật, hãy dùng URL Kaggle thật.

## Chạy local stack

```bash
docker compose up -d --build
docker compose ps
```

Các endpoint:

- API Gateway: http://localhost:8000
- API docs: http://localhost:8000/docs
- Prefect UI: http://localhost:4200
- Grafana: http://localhost:3000 (`admin` / `admin`)
- Prometheus: http://localhost:9090
- Qdrant dashboard: http://localhost:6333/dashboard

## Chạy pipeline

Gửi sample data vào Kafka:

```bash
python scripts/01_ingest_to_kafka.py
```

Prefect worker service sẽ tự chạy `prefect/flows/kafka_to_delta.py` theo chu kỳ `FLOW_INTERVAL_SECONDS` để:

1. consume topic `data.raw`;
2. ghi batch vào `delta-lake/raw`; nếu có `pandas/pyarrow` thì ghi parquet, nếu không thì dùng JSONL fallback nhẹ cho môi trường lab;
3. ghi feature vào Redis với key `feature:<id>`;
4. tạo/upsert vector vào Qdrant collection `documents`.

Có thể chạy thủ công từng bước:

```bash
python scripts/03_delta_to_feast.py
python scripts/05_embed_to_qdrant.py
```

## Gọi API

Health:

```bash
curl http://localhost:8000/health
```

Chat:

```bash
curl -X POST http://localhost:8000/api/v1/chat ^
  -H "Content-Type: application/json" ^
  -d "{\"query\":\"What is platform engineering?\",\"embedding\":[0.1,0.1,0.1]}"
```

API sẽ tự gọi embedding service nếu không truyền vector đủ 384 chiều. Nếu Qdrant lỗi, API dùng context rỗng. Nếu vLLM/ngrok lỗi và `ALLOW_LLM_FALLBACK=true`, API trả fallback answer để demo graceful degradation.

## Kiểm tra

```bash
python -m compileall api-gateway scripts prefect/flows smoke-tests
docker compose config --quiet
pytest smoke-tests/ -v
python scripts/production_readiness_check.py
python scripts/09_verify_observability.py
```

Mục tiêu:

- Smoke tests pass.
- Production readiness score >= 80%.
- Prometheus scrape được `api-gateway`.
- Grafana health OK.
- Qdrant có collection `documents`.
- Redis có key `feature:*`.

Kiểm tra strict cho native vLLM trên Kaggle:

```bash
python scripts/10_verify_kaggle_vllm.py
```

Script này phải pass nếu muốn chứng minh `/v1/models`, `/v1/chat/completions`, `/embed` và API local đều đang dùng Kaggle vLLM thật. Nếu Kaggle/ngrok đang lỗi tạm thời, smoke tests và readiness vẫn chứng minh phần platform local, observability, vector store, feature store và graceful fallback.

## Kaggle Notebook gợi ý

Notebook nộp bài chính là `kaggle/lab28_kaggle_bootstrap.ipynb`. Notebook này chạy vLLM thật ở port `8001`, chạy gateway ở port `8000`, expose gateway bằng ngrok và dùng chung một public URL cho cả:

```env
VLLM_NGROK_URL=https://your-gateway.ngrok-free.app
EMBED_NGROK_URL=https://your-gateway.ngrok-free.app
MODEL_NAME=Qwen/Qwen2.5-0.5B-Instruct
ALLOW_LLM_FALLBACK=true
```

Vẫn còn `kaggle/lab28_kaggle_compat_server.ipynb` để debug khi vLLM lỗi. Notebook compat giúp chứng minh end-to-end flow khi Kaggle vLLM không ổn định, nhưng bằng chứng vLLM thật là `kaggle/lab28_kaggle_bootstrap.ipynb` cộng với `scripts/10_verify_kaggle_vllm.py`.

Embedding API cần nhận:

```json
{"texts": ["hello"]}
```

và trả:

```json
{"embeddings": [[0.1, 0.2]]}
```

## Troubleshooting

- `api-gateway` exited: chạy `docker compose logs api-gateway`; thường do image chưa rebuild hoặc `.env` sai.
- Prefect UI không lên: kiểm tra `docker compose logs prefect-orion`; command đúng là `prefect server start --host 0.0.0.0`.
- Worker không xử lý data: kiểm tra `docker compose logs prefect-worker` và topic Kafka bằng `docker compose exec -T kafka kafka-topics --list --bootstrap-server localhost:9092`.
- Qdrant chưa có data: chạy lại `python scripts/01_ingest_to_kafka.py`, đợi một chu kỳ worker, rồi mở http://localhost:6333/dashboard.

## Artifact nộp bài

Xem `SUBMISSION.md`.
