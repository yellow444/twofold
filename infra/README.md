# Инфраструктура и проверки

В папке `infra/` размещаются артефакты для локального CI, включая единый `docker-compose.yml`, описывающий процессы линтинга, тестирования, сборки и публикации контейнеров всех компонент платформы.

## Docker Compose

`docker-compose.yml` содержит по четыре сервиса для каждого компонента (`lint`, `test`, `build`, `publish`) и использует профили Compose, чтобы запускать только нужный тип проверок. Все сервисы работают в общей сети `ci` и делят два тома:

* `build-cache` — кеш промежуточных файлов и зависимостей;
* `build-artifacts` — итоговые артефакты (логи линтинга, отчёты тестов, сборки, журналы публикаций).

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
