# Hướng Dẫn Nộp Bài - Lab 28

## Yêu cầu

Nộp một repo GitHub chứa source code hoàn chỉnh và bằng chứng demo end-to-end của AI platform hybrid.

## Artifact cần nộp

```text
lab28_submission_<student_id>/
├── lab28/
│   ├── docker-compose.yml
│   ├── api-gateway/
│   ├── prefect/flows/
│   ├── scripts/
│   ├── monitoring/
│   ├── smoke-tests/
│   ├── README.md
│   └── .env.example
├── screenshots/
│   ├── prefect_ui.png
│   ├── api_gateway_health_or_chat.png
│   └── grafana_dashboard.png
├── smoke_tests_results.png
└── production_readiness.png
```

Không commit `.env`, token ngrok, LangSmith key hoặc secret khác.

## Lệnh cần chạy trước khi chụp kết quả

```bash
docker compose up -d --build
python scripts/01_ingest_to_kafka.py
pytest smoke-tests/ -v
python scripts/production_readiness_check.py
python scripts/09_verify_observability.py
```

Kỳ vọng:

- `docker compose ps`: các service chính đều `Up`.
- `pytest smoke-tests/ -v`: pass toàn bộ tests.
- `production_readiness_check.py`: score >= 80%.
- `09_verify_observability.py`: Prometheus OK; LangSmith OK nếu đã cấu hình key.
- Grafana, Prometheus, Prefect UI truy cập được.

Kiểm tra thêm cho native Kaggle vLLM:

```bash
python scripts/10_verify_kaggle_vllm.py
```

Khi Kaggle vLLM chạy ổn, script này pass và chứng minh `/v1/models`, `/v1/chat/completions`, `/embed` cùng API local đều dùng vLLM thật. Nếu Kaggle session/ngrok bị lỗi sát giờ nộp, vẫn nộp repo với screenshot smoke tests, readiness, Grafana/Prefect/API và ghi chú rằng repo có notebook vLLM thật kèm script strict validation.

## Tiêu chí chấm điểm

| Tiêu chí | Trọng số | Mô tả |
| --- | ---: | --- |
| Integration completeness | 40% | 10 integration points hoạt động, data đi từ Kafka đến API/observability |
| Observability | 25% | Metrics, logs, traces và dashboard thể hiện được trạng thái hệ thống |
| Performance | 20% | API có timeout, latency hợp lý, xử lý lỗi không làm crash service |
| Architecture quality | 15% | Cấu hình rõ ràng, separation tốt, có fallback và tài liệu dễ chạy lại |

## 5 câu hỏi cần chuẩn bị

1. Trade-off kiến trúc: Kafka tăng độ phức tạp nhưng giúp decouple, replay và scale từng component độc lập; Kaggle giảm chi phí GPU nhưng cần fallback khi ngrok/GPU lỗi.
2. Khi Kaggle mất kết nối: API timeout có kiểm soát, Qdrant lỗi dùng context rỗng, vLLM lỗi có fallback answer nếu `ALLOW_LLM_FALLBACK=true`.
3. Event-driven với Kafka: producer chỉ gửi event vào topic, Prefect consume độc lập, các bước Delta/Redis/Qdrant có thể retry mà không chặn ingestion.
4. Observability: FastAPI expose `/metrics`, Prometheus scrape API, Grafana hiển thị health/metrics, Prefect UI hiển thị flow runs, LangSmith ghi trace khi có API key.
5. Khi service crash: Docker restart policy tự khởi động lại, API vẫn healthy nếu Qdrant/vLLM lỗi một phần, pipeline có thể chạy lại và upsert idempotent vào Redis/Qdrant.

## Checklist cuối

- [ ] `.env.example` có đủ biến, `.env` không bị commit.
- [ ] API Gateway `/health`, `/docs`, `/metrics` OK.
- [ ] Kafka topic `data.raw` tồn tại.
- [ ] Prefect flow có run trong UI.
- [ ] Redis có `feature:*`.
- [ ] Qdrant collection `documents` có points.
- [ ] Smoke tests pass.
- [ ] Readiness score >= 80%.
- [ ] Có đủ screenshot nộp bài.
