# agent.md — Руководство и спецификация AI‑агента проекта «Сервис анализа активности БПЛА»

Документ описывает цели агента, протоколы взаимодействия, инструменты, политики, сценарии эксплуатации и тестирования. Файл предназначен для размещения в корне репозитория и чтения при старте контейнеров.

---

## 1. Назначение и зона ответственности

**Цель:** автоматизировать обработку данных Росавиации и выпуск объяснимых аналитических витрин по полётной активности гражданских БПЛА по регионам РФ.

**Роли агентов:**

* **Agent.Ingest** — загрузка и нормализация (XLS/CSV/PDF/HTML → Parquet/CSV → PostgreSQL).
* **Agent.Quality** — валидация схем/диапазонов, поиск пропусков/дубликатов/выбросов, отчёт качества.
* **Agent.Geo** — геопривязка к субъектам РФ (PostGIS, пересечение точек и полигонов).
* **Agent.Aggregate** — расчёт агрегатов и материализованных представлений.
* **Agent.Insights** — генерация кратких человеко‑читабельных пояснений (LLM/правила) без влияния на числа.
* **Agent.ChangeWatcher** — слежение за новыми партиями, дифф версий датасетов, авто‑пересчёт.

> Каждый агент — изолированный контейнер, общение через REST/очередь и таблицы статусов в БД.

---

## 2. Архитектура и запуск

```
repo/
  backend/           # ASP.NET Core Minimal APIs (Swagger)
  frontend/          # React + TS (Vite)
  agents/
    ingest/
    quality/
    geo/
    aggregate/
    insights/
    changewatcher/
  infra/
    docker-compose.yml
    keycloak/
    postgres/
    minio/
    grafana/
  docs/
  agent.md           # этот файл
```

**Запуск локально:**

```bash
# 1) подготовить окружение
cp .env.example .env
# 2) поднять инфраструктуру
docker compose -f infra/docker-compose.yml up -d postgres
# 3) применить миграции БД (команда зависит от выбранного инструмента)
docker compose -f infra/docker-compose.yml run --rm backend-build ./scripts/migrate.sh
# 4) прогнать пробный ingest
docker compose run --rm agent-ingest python -m app.cli ingest ./samples/rosaviation_2023.csv
# 5) пересчитать агрегаты
docker compose run --rm agent-aggregate python -m app.cli aggregate --year 2023
```

**Компоненты (open‑source и бесплатные):** PostgreSQL + PostGIS, MinIO (S3‑совместимое хранилище), Keycloak (OIDC), Prometheus/Grafana/Jaeger/Loki, ClickHouse (опционально), DuckDB/Parquet, Python (pandas/polars), ASP.NET Core, React+TS.

---

## 3. Конфигурация и переменные окружения

Создать `.env` в корне (пример значений):

```
# Бэкенд/API
ASPNETCORE_URLS=http://0.0.0.0:8080
JWT_AUTHORITY=http://keycloak:8081/realms/bpla
JWT_AUDIENCE=bpla-api

# Базы и хранилище
POSTGRES_HOST=postgres
POSTGRES_DB=bpla
POSTGRES_USER=bpla_user
POSTGRES_PASSWORD=dev_password_123
CLICKHOUSE_URL=http://clickhouse:8123
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=local_minio_key
MINIO_SECRET_KEY=local_minio_secret
MINIO_BUCKET=datasets

# LLM (локально/он‑прем, при необходимости)
LLM_PROVIDER=local
LLM_MODEL=ggml-small
LLM_MAX_TOKENS=512

# Трассировка/метрики
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
PROMETHEUS_PUSHGATEWAY=http://pushgateway:9091
```

> Секреты в проде храним в Secret Manager/внешнем Vault, доступ в контейнерах через env.

---

## 4. Протокол обмена (общая схема сообщений)

**Очередь/шина (опционально Redis Streams/RabbitMQ):**

```json
{
  "event": "dataset.ingested|dataset.validated|aggregates.built|insights.ready",
  "version": "2023.12.31-01",
  "payload": {
    "year": 2023,
    "source_uri": "s3://datasets/rosaviation/2023.csv",
    "records": 10234,
    "quality_status": "OK|WARN|FAIL"
  },
  "trace_id": "a3bd5c...",
  "emitted_at": "2025-09-30T09:15:12Z"
}
```

**Состояния партий в БД:** `dataset_version(id, year, source_uri, status, checksum, ingested_at, validated_at, aggregated_at)`.

---

## 5. API (сжатая спецификация)

* `POST /ingest` — загрузить партию (multipart/JSON, возвращает `dataset_version`).
* `GET  /datasets/{version}` — статус партии и отчёт качества.
* `POST /aggregate?year=YYYY` — пересчитать агрегаты.
* `GET  /regions` — справочник регионов.
* `GET  /stats?year=YYYY&region_id=ID` — агрегаты (count, sum_duration, avg, median, monthly).
* `GET  /insights?year=YYYY&region_id=ID` — человеко‑читабельные пояснения.
* `GET  /export?format=csv|xlsx|json|png|jpeg` — выгрузки.

Swagger доступен по `/swagger`.

---

## 6. Алгоритмы и правила

* **Нормализация времени:** все отметки в UTC; парсинг локальных временных зон приводится к UTC.
* **Дубликаты:** ключ `(flight_id, start_time, region_guess)`; при совпадении — последняя запись побеждает, предыдущая помечается `superseded=true`.
* **Геопривязка:** `ST_Point(lon, lat)` → `ST_Intersects(point, region_polygon)`; точки на границе — правило «центр тяжести полигона ближайшего субъекта» (документировать в `docs/methodology.md`).
* **Аномалии:** нулевая длительность при `count>0`; отрицательная длительность; координаты вне [−90..90]/[−180..180]; даты вне года партии.
* **Инсайты:** правила + LLM‑суммари (например, «Рост вылетов на 18% г/г, средняя длительность снизилась на 6%. Пик — июль.»).

---

## 7. Инструменты агентов

* **Файлы:** чтение/запись в MinIO (S3‑совместимый API).
* **БД:** PostgreSQL/PostGIS для нормализованных данных и витрин; ClickHouse для быстрых срезов (по мере необходимости).
* **LLM:** локальная модель/контур он‑прем (без отправки данных наружу).
* **Логи/метрики:** структурные логи JSON; Prometheus‑метрики; OpenTelemetry‑трейсы.

---

## 8. Шаблоны промптов (Agent.Insights)

```
Суммируй факты о регионе:
— Период: {year}
— Вылеты: {flights_count}
— Суммарная длительность (ч): {duration_total_h}
— Средняя длительность (мин): {avg_min}
— ТОП‑месяц: {top_month} ({top_month_count} вылетов)
Задача: кратко (≤ 2 предложения) объясни тренд г/г и сезонный пик. Не придумывай данных, используй только числа выше.
```

```
Проверь аномалии:
— Нулевая длительность при {flights_count} вылетов
— Координаты вне допустимых диапазонов: {bad_coords}
— Записи вне календарного года: {out_of_year}
Выведи список пунктов, которые требуют ручной проверки. Кратко и по делу.
```

---

## 9. Политики и ограничения

* **Explainable‑by‑design:** все агрегаты раскрываемы до первичных записей; формулы доступны в UI.
* **LLM не влияет на числа:** только на текст‑пояснения и подсказки.
* **Данные остаются внутри контура:** без внешних API для персональных/первичных данных.
* **Версионирование:** каждая партия неизменяема; исправления только новой версией.

---

## 10. Мониторинг и оповещения

* **Метрики:** `etl_duration_seconds`, `quality_fail_count`, `geocoded_ratio`, `aggregate_runtime_seconds`, `insights_latency_ms`.
* **Алерты:**

  * `quality_fail_count > 0` — уведомление в канал операций.
  * `geocoded_ratio < 0.99` — предупреждение.
  * `etl_duration_seconds > SLA` — предупреждение.

---

## 11. Тесты и верификация

* **Unit:** парсинг форматов, дедупликация, геопривязка, расчёт агрегатов.
* **Integration:** end‑to‑end прогон партии (samples → БД → агрегаты → инсайты).
* **UI (Playwright):** дашборд, карта, карточка региона, экспорт.
* **Нагрузка (k6/Locust):** цель — обработка 10 000 записей ≤ 5 минут локально.

---

## 12. DevOps и CI/CD

* **Docker Compose:**

  * `build`: сборка контейнеров, линт, тесты, публикация образов.
  * `migrate`: применение миграций в стейдже.
  * `e2e`: прогон end‑to‑end с сэмплами.
  * `release`: упаковка артефактов (Swagger JSON, SQL‑миграции, docs PDF).
* **Версионирование:** semver тегом; образы `org/bpla-<component>:<version>`.

---

## 13. Операционные процедуры (runbook)

* **Поступила новая партия:** Agent.ChangeWatcher фиксирует файл → запускает Ingest → Quality → Geo → Aggregate → Insights → публикует событие и ссылку на отчёт.
* **Quality=FAIL:** агент помечает проблемные записи, останавливает пайплайн, создаёт задачу на разбор.
* **Инцидент производительности:** проверить последние деплои, планы запросов, индексы, нагрузку диска/сети; при необходимости — временно отключить ClickHouse и сводить в Postgres.

---

## 14. Лицензирование и соответствие

* Все компоненты — open‑source и бесплатные (Apache‑2.0/MIT/BSD/GPL, в зависимости от проекта). Список лицензий хранится в `docs/licenses.md`.

---

## 15. Контрольный чек‑лист готовности агента (Go‑Live)

* [ ] БД и миграции применяются, индексы актуальны
* [ ] Сэмплы 2023/2024 прогоняются end‑to‑end без ошибок
* [ ] Геопривязка ≥ 99%
* [ ] Swagger доступен, Postman‑коллекция обновлена
* [ ] Мониторинг/алерты работают, есть базовый дашборд SLA
* [ ] Документация `docs/` собрана (MkDocs/Docusaurus), PDF приложен
