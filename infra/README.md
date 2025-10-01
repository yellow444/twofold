# Инфраструктура и проверки

В папке `infra/` размещаются артефакты для локального CI, включая единый `docker-compose.yml`, описывающий процессы линтинга, тестирования, сборки и публикации контейнеров всех компонент платформы.

## Docker Compose

`docker-compose.yml` содержит по четыре сервиса для каждого компонента (`lint`, `test`, `build`, `publish`) и использует профили Compose, чтобы запускать только нужный тип проверок. Все сервисы работают в общей сети `ci` и делят два тома:

* `build-cache` — кеш промежуточных файлов и зависимостей;
* `build-artifacts` — итоговые артефакты (логи линтинга, отчёты тестов, сборки, журналы публикаций).

Дополнительно определён сервис `postgres` на базе PostGIS, который хранит данные в отдельном томе `postgres-data` и инициализирует расширение `postgis` при старте.

### Примеры запуска

```bash
# Линтинг всех компонентов
for service in backend frontend agent-aggregate agent-changewatcher agent-geo agent-ingest agent-insights agent-quality; do
  docker compose -f infra/docker-compose.yml --profile lint run --rm "${service}-lint"
done

# Юнит-тесты (пример: агент ingest)
docker compose -f infra/docker-compose.yml --profile test run --rm agent-ingest-test

# Сборка фронтенда
docker compose -f infra/docker-compose.yml --profile build run --rm frontend-build

# Публикация артефактов backend
docker compose -f infra/docker-compose.yml --profile publish run --rm backend-publish
```

Каждая команда использует встроенные скрипты внутри образов, которые создают артефакт в `/artifacts`. Артефакты доступны после выполнения контейнера, так как том `build-artifacts` сохраняется между запусками. Для инспекции содержимого можно выполнить, например:

```bash
docker run --rm -v twofold_build-artifacts:/artifacts busybox ls -R /artifacts
```

> Название тома в Docker будет формироваться как `<папка>_<имя тома>` (для репозитория `twofold` — `twofold_build-artifacts`).

### Настройка профилей

Профили позволяют гибко вызывать только необходимые проверки:

* `lint` — статический анализ и базовые проверки качества кода;
* `test` — юнит-тесты и smoke-проверки;
* `build` — сборка артефактов и подготовка пакетов;
* `publish` — симуляция публикации/выгрузки артефактов.

Команды можно комбинировать, передавая несколько профилей одновременно (`--profile lint --profile test`).

## Структура каталогов

Каждый компонент (backend, frontend, агенты) содержит `Dockerfile` и набор скриптов `scripts/*.sh`, которые отвечают за запуск соответствующих команд. Скрипты работают с переменными окружения `COMPONENT_NAME`, `ARTIFACTS_DIR` и `CACHE_DIR`, что позволяет при необходимости переопределить пути при интеграции с внешними CI/CD системами.

## PostgreSQL/PostGIS локально

1. Скопируйте пример переменных окружения и при необходимости подставьте собственные значения:

   ```bash
   cp .env.example .env
   ```

2. Поднимите базу данных и дождитесь статуса `healthy`:

   ```bash
   docker compose -f infra/docker-compose.yml up -d postgres
   docker compose -f infra/docker-compose.yml ps postgres
   ```

3. После успешного старта примените SQL-миграции из каталога `infra/postgres/migrations/`:

   ```bash
   ./infra/postgres/migrate.sh
   ```

   Скрипт копирует миграции в контейнер `postgres` и применяет их последовательно через `psql` (с флагом `ON_ERROR_STOP`), выводя лог об успешных шагах.

4. Для smoke-проверки геозапросов и годовых агрегатов выполните:

   ```bash
   ./infra/postgres/smoke.sh
   ```

   SQL-скрипт (`infra/postgres/smoke.sql`) запускает выборки с `ST_Intersects` и сверяет агрегаты `aggregates_year` с нормализованными полётами, что подтверждает корректность DoD по PostGIS и витринам.
