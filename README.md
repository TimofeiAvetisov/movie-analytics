# Movie Analytics

Pipeline событий онлайн-кинотеатра: HTTP/генератор → Kafka (Avro + Schema Registry) → ClickHouse Kafka Engine → MergeTree → сервис агрегации → PostgreSQL → S3 (MinIO). Grafana читает материализованные агрегаты ClickHouse.

## Запуск

```bash
docker compose up --build
```

Все сервисы, топик, схема версии 1, таблицы, бакет и дашборд создаются автоматически. Первый запуск загружает Docker-образы. Адреса:

- Producer API: `http://localhost:8001/docs`
- Aggregation API: `http://localhost:8002/docs`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- MinIO Console: `http://localhost:9011` (`analytics` / `analytics-secret`)

В `movie-events` три партиции, replication factor 2, `min.insync.replicas=1`. Три брокера обеспечивают кворум контроллеров KRaft при отказе одного узла. Ключ сообщения — `user_id`: все действия одного пользователя попадают в одну партицию и читаются в порядке публикации. Это важно для последовательности просмотра и расчёта первого просмотра/retention. Продюсер использует `acks=all`, идемпотентную запись и повторы с экспоненциальной задержкой. Avro-схема находится в `schema/movie-event-v1.avsc`, субъект Schema Registry — `movie-events-value`, версия — 1.

Генератор включён автоматически и каждые 5 секунд создаёт сессию `VIEW_STARTED → VIEW_PAUSED → VIEW_RESUMED → VIEW_FINISHED` с неубывающим прогрессом; иногда добавляет `LIKED` и `SEARCHED`. Режим отключается переменной `GENERATE_EVENTS=0` для сервиса `producer`.

## Ручные запросы

Отправить событие:

```bash
curl -X POST http://localhost:8001/events -H 'Content-Type: application/json' -d '{"event_id":"aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa","user_id":"user-1","movie_id":"movie-1","event_type":"VIEW_STARTED","timestamp":"2026-11-25T12:00:00Z","device_type":"MOBILE","session_id":"session-1","progress_seconds":0}'
```

Пересчитать и выгрузить конкретный день:

```bash
curl -X POST 'http://localhost:8002/recalculate?date=2026-11-25'
curl -X POST 'http://localhost:8002/export?date=2026-11-25'
```

Агрегация запускается при старте и каждые `AGGREGATION_INTERVAL_MINUTES` минут (по умолчанию 5). Она пересчитывает сегодня и предыдущие 7 дней, чтобы D7 обновлялся после возвращения пользователей, и экспортирует вчерашние метрики. При ошибке экспорта следующая плановая попытка повторит его. Путь объекта: `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.csv`; повторная выгрузка заменяет объект.

Метрики по дням: DAU (`uniqExact(user_id)` всех событий), средняя длительность завершённого просмотра (`avgIf(progress_seconds)`), просмотры фильмов (`countIf(VIEW_STARTED)`), конверсия (`VIEW_FINISHED / VIEW_STARTED`), D1/D7 (доля когорты пользователей с первым `VIEW_STARTED` в этот день, снова запустивших просмотр через 1/7 дней). `cohort_retention` хранит Day 0–7. Для результатов в ClickHouse используется `ReplacingMergeTree`; дашборд читает `FINAL`. PostgreSQL использует первичный ключ `(date, metric_name, dimension)` и `ON CONFLICT DO UPDATE`.

## Проверка

```bash
docker compose run --build --rm e2e
```

Команда автоматически запускает зависимости, публикует уникальное событие через producer и проверяет все его поля в ClickHouse. Тест можно повторять: событие получает новый UUID.

Этот Compose предназначен для локальной демонстрации: пароли заданы в файле, сервисы работают в одной сети Docker.
