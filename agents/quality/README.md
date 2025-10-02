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

Проверка конкретной версии датасета и запись отчёта:

```bash
python -m agents.quality.app.cli validate --dataset-version 2024-Q1 \
  --output artifacts/quality_report_2024-Q1.json
```

Команда выводит итоговый статус `OK`, `WARN` или `FAIL` в stdout. Выходной код `1` возвращается только при статусе `FAIL`.

Для быстрого запуска без сохранения отчёта используйте флаг `--dry-run`:

```bash
python -m agents.quality.app.cli validate --dataset-version 2024-Q1 --dry-run
```

## Интерпретация отчёта

JSON-отчёт содержит список проверок с описанием и деталями:

```json
{
  "dataset_version": "2024-Q1",
  "generated_at": "2024-03-15T12:30:01+00:00",
  "status": "WARN",
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
  ]
}
```

- `status` — агрегированный итог по всем проверкам (при наличии хотя бы одного `FAIL`).
- `checks` — результаты индивидуальных проверок: схема, диапазоны, уникальность, полнота месяцев, выбросы.
- Поле `details` помогает быстро локализовать проблемные строки или диапазоны.

## Скрипты

Для единообразных проверок доступны shell-скрипты:

- `scripts/lint.sh` — запуск `ruff` и `black --check`.
- `scripts/test.sh` — модульные тесты (`pytest`).
- `scripts/build.sh` — сборка wheel-пакета.
- `scripts/publish.sh` — сборка wheel и sdist в артефакты.

Все команды используют переменную `ARTIFACTS_DIR` (по умолчанию `/artifacts`).
