# Руководство для контрибьюторов

Спасибо за интерес к проекту «Сервис анализа активности БПЛА». Этот документ описывает базовые правила работы с репозиторием и помогает сохранить единый процесс для всех команд.

## Общие принципы

* Все изменения выполняются через pull request (PR), прямые коммиты в `main` запрещены.
* Перед началом работы убедитесь, что требования и артефакты этапа указаны в [plan.md](plan.md); PR должен ссылаться на актуальный пункт плана.
* Для задач из нескольких компонент разбивайте изменения на независимые PR, чтобы упростить ревью.

## Ветвление и pull request

1. Создайте ветку от `main`, используя префиксы по типу работы:
   * `feature/<краткое-описание>` — новые возможности;
   * `bugfix/<идентификатор>` — исправления ошибок;
   * `hotfix/<идентификатор>` — срочные исправления в продуктиве;
   * `docs/<тема>` — документация.
2. В описании PR укажите:
   * суть изменений и связанные задачи/тикеты;
   * компонент(ы), которых коснулось изменение;
   * чек-лист запусков проверок.
3. Минимум два аппрува от владельцев областей из `CODEOWNERS` требуется перед слиянием.
4. PR должен быть актуален с `main` (при необходимости выполните rebase или merge).

## Обязательные проверки

Перед созданием PR локально выполните следующие команды и приложите результаты в описание:

```bash
# Линтеры всех компонент
for service in backend frontend agent-aggregate agent-changewatcher agent-geo agent-ingest agent-insights agent-quality; do
  docker compose -f infra/docker-compose.yml --profile lint run --rm "${service}-lint"
done

# Юнит-тесты (дополните при появлении реальных тестов)
for service in backend frontend agent-aggregate agent-changewatcher agent-geo agent-ingest agent-insights agent-quality; do
  docker compose -f infra/docker-compose.yml --profile test run --rm "${service}-test"
done

# Сборка и публикация артефактов
for service in backend frontend agent-aggregate agent-changewatcher agent-geo agent-ingest agent-insights agent-quality; do
  docker compose -f infra/docker-compose.yml --profile build run --rm "${service}-build"
  docker compose -f infra/docker-compose.yml --profile publish run --rm "${service}-publish"
done
```

> Скрипты внутри контейнеров создают артефакты в томе `build-artifacts`. При появлении полноценных проектов замените заглушки на реальные линтеры/тесты, но сохраните интерфейс команд для единообразия CI.

## Правила коммитов

* Используйте формат [Conventional Commits](https://www.conventionalcommits.org/ru/v1.0.0/) (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:` и т.д.).
* Один коммит — одна логическая единица работы. Не смешивайте refactor и функциональные изменения в одном коммите.
* Сообщение коммита должно быть на английском языке, тело — по необходимости.

## Проверка соответствия плану

* На стадии ревью владельцы из `CODEOWNERS` сверяют изменения с требованиями этапа, описанными в [plan.md](plan.md).
* Если изменения затрагивают несколько этапов, опишите в PR стратегию интеграции и потенциальные риски.
* Документацию в `docs/` обновляйте одновременно с кодом, чтобы артефакты оставались консистентными.

Спасибо за вклад в развитие проекта!
