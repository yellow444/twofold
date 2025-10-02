# Agent.Quality

Модуль проверки качества нормализованных данных о рейсах. Содержит пайплайн проверки, CLI и набор скриптов для локальной разработки.

## Настройка окружения

1. Перейдите в каталог компонента:
   ```bash
   cd agents/quality
   ```
2. Установите зависимости (рекомендуется использовать virtualenv/pyenv):
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -e .[checks]
   ```
   Доп. зависимость `checks` подключает Great Expectations при необходимости.

## Конфигурация

CLI использует переменные окружения с префиксом `QUALITY_`:

| Переменная | Назначение | Значение по умолчанию |
| --- | --- | --- |
| `QUALITY_DATABASE_URL` | Строка подключения к Postgres | — (обязательно) |
| `QUALITY_DATABASE_SCHEMA` | Схема БД с нормализованными рейсами | `public` |
| `QUALITY_TABLE_NAME` | Таблица с рейсами в канонической схеме | `normalized_flights` |
| `QUALITY_ARTIFACTS_DIR` | Директория для отчётов | `./artifacts` |
| `QUALITY_DEFAULT_DATASET_VERSION` | Значение версии по умолчанию | `None` |

## Запуск CLI

Проверка конкретной версии датасета, запись отчёта и обновление Postgres:

```bash
python -m agents.quality.app.cli validate --dataset-version 2024-Q1 \
  --db-host db.internal --db-port 5432 --db-user qa_bot --db-password secret
```

CLI принимает параметры подключения напрямую (`--database-url`, `--db-host`, `--db-port`, `--db-name`, `--db-user`, `--db-password`, `--db-sslmode`). Они дополняют или переопределяют `QUALITY_DATABASE_URL` и позволяют запускать агент в разных окружениях без изменения переменных.

Команда печатает итоговый `quality_status` (`validated`, `quality_warn`, `quality_fail`) в stdout. Выходной код `1` возвращается только при фактическом провале проверок (`quality_fail`).

Флаг `--dry-run` отключает запись в БД, но артефакты (`quality_report_*.json`, `quality_violations_*.json`) продолжают сохраняться в директорию `ARTIFACTS_DIR`:

```bash
python -m agents.quality.app.cli validate --dataset-version 2024-Q1 --dry-run
```

## Интерпретация отчёта

JSON-отчёт содержит агрегированную информацию о проверках и сохранён во вложении `quality_report_<версия>.json`:

```json
{
  "dataset_version": "2024-Q1",
  "generated_at": "2024-03-15T12:30:01+00:00",
  "status": "WARN",
  "quality_status": "quality_warn",
  "warn_count": 1,
  "fail_count": 0,
  "checks": [
    {
      "name": "monthly_completeness",
      "status": "WARN",
      "summary": "missing intermediate months detected",
      "details": {
        "missing_months": {"2024": [2]},
        "observed": {"2024": [1, 3]}
      }
    },
    {
      "name": "duration_range",
      "status": "OK",
      "summary": "all durations within expected range"
    }
  ],
  "entries": [
    {
      "check_name": "monthly_completeness",
      "severity": "WARN",
      "details": {
        "summary": "missing intermediate months detected",
        "details": {
          "missing_months": {"2024": [2]},
          "observed": {"2024": [1, 3]}
        }
      }
    },
    {
      "check_name": "duration_range",
      "severity": "OK",
      "details": {
        "summary": "all durations within expected range"
      }
    }
  ]
}
```

- `status` — агрегированный итог по статусам `OK`/`WARN`/`FAIL`.
- `quality_status` — значение, записываемое в `dataset_version.status`.
- `warn_count`/`fail_count` — количество проверок в соответствующих состояниях.
- `entries` — нормализованные строки, которые попадают в таблицу `quality_report` (каждый объект содержит `summary`, исходные `details` и списки затронутых записей/регионов, если они определены).
- Дополнительный файл `quality_violations_<версия>.json` содержит краткую сводку: суммарные WARN/FAIL и финальный `quality_status`.

### Обновление статусов в Postgres

- Каждому запуску соответствует удаление прежних записей `quality_report` для выбранной версии и вставка актуальных результатов.
- В таблице `dataset_version` обновляются поля `status`, `validated_at`, `quality_warn_count`, `quality_fail_count`. Значения статуса: `validated` (все проверки OK), `quality_warn` (есть WARN), `quality_fail` (есть FAIL).
- При запуске с `--dry-run` вставки и обновления в БД не выполняются.

## Скрипты

Для единообразных проверок доступны shell-скрипты:

- `scripts/lint.sh` — запуск `ruff` и `black --check`.
- `scripts/test.sh` — модульные тесты (`pytest`).
- `scripts/build.sh` — сборка wheel-пакета.
- `scripts/publish.sh` — сборка wheel и sdist в артефакты.

Все команды используют переменную `ARTIFACTS_DIR` (по умолчанию `/artifacts`).
