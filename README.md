# Stream Stats Aggregator API

Backend-сервис на FastAPI для получения, расчёта и агрегации статистики из внешнего API.

Проект предназначен для получения статистических данных за выбранные временные интервалы, расчёта значений за отдельный рабочий период, бонусного периода между периодами и общей суммы `stream + bonus`.

Сервис рассчитан на работу на VPS и включает защиту от перегрузки внешнего API: очередь запросов, ограничение конкурентности, глобальный rate limit, retries, backoff и jitter.

## Стек

- Python
- FastAPI
- Pydantic
- httpx
- asyncio
- Uvicorn
- Linux VPS
- systemd

## Возможности

- Получение статистики за выбранный временной интервал
- Расчёт бонусного периода между двумя рабочими периодами
- Расчёт общей суммы `stream + bonus`
- Поддержка разных часовых поясов через `zoneinfo`
- Валидация входных данных через Pydantic
- Асинхронные запросы к внешнему API через `httpx`
- Ограничение конкурентности через `asyncio.Semaphore`
- Очередь запросов с timeout
- Глобальный rate limit между внешними API-вызовами
- Retry-механизм для сетевых ошибок и 5xx-ответов
- Обработка HTTP 429 Too Many Requests
- Exponential backoff и jitter
- Health-check endpoint
- Endpoint для просмотра состояния очереди

## Структура проекта

```text
stream-stats-aggregator-api/
├── README.md
├── app.py
├── requirements.txt
├── .env.example
├── .gitignore
└── systemd/
    └── stream-stats-api.service.example
```

## Основные endpoints

### Health-check

```http
GET /healthz
```

Возвращает состояние сервиса.

### Статистика очереди

```http
GET /queue-stats
```

Показывает количество активных задач и доступную ёмкость очереди.

### Получить токены за период

```http
POST /stats/tokens-by-window
```

Пример тела запроса:

```json
{
  "model_username": "example_model",
  "period_start": "2026-04-23T10:00:00",
  "period_end": "2026-04-23T18:00:00",
  "tz_name": "Europe/Moscow"
}
```

### Получить бонус между периодами

```http
POST /stats/bonus-between
```

Пример тела запроса:

```json
{
  "model_username": "example_model",
  "prev_stream_end": "2026-04-23T01:00:00",
  "next_stream_start": "2026-04-23T10:00:00",
  "tz_name": "Europe/Moscow",
  "mode": "time"
}
```

### Рассчитать stream + bonus

```http
POST /stats/stream-and-bonus
```

Пример тела запроса:

```json
{
  "model_username": "example_model",
  "period_start": "2026-04-23T10:00:00",
  "period_end": "2026-04-23T18:00:00",
  "prev_stream_end": "2026-04-23T01:00:00",
  "tz_name": "Europe/Moscow",
  "bonus_mode": "time"
}
```

## Установка

```bash
git clone https://github.com/your-username/stream-stats-aggregator-api.git
cd stream-stats-aggregator-api

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
```

После этого нужно заполнить `.env`.

## Пример `.env`

```env
EXTERNAL_API_KEY=change_me
STUDIO_USERNAME=example_studio

EXTERNAL_API_BASE_URL=https://example.com
EXTERNAL_API_PATH_TEMPLATE=/api/stats/v2/studios/username/{studio}/models/username/{model}

MAX_CONCURRENCY=1
QUEUE_TIMEOUT=900
POST_JOB_COOLDOWN=1.5
GLOBAL_MIN_INTERVAL=1.2
MAX_RETRIES=4
BACKOFF_BASE=1.8
JITTER_MAX=0.3
```

## Локальный запуск

```bash
uvicorn app:app --host 127.0.0.1 --port 8080
```

Swagger-документация будет доступна по адресу:

```text
http://127.0.0.1:8080/docs
```

## Пример запроса

```bash
curl -X POST "http://127.0.0.1:8080/stats/stream-and-bonus" \
  -H "Content-Type: application/json" \
  -d '{
    "model_username": "example_model",
    "period_start": "2026-04-23T10:00:00",
    "period_end": "2026-04-23T18:00:00",
    "prev_stream_end": "2026-04-23T01:00:00",
    "tz_name": "Europe/Moscow",
    "bonus_mode": "time"
  }'
```

## Деплой на VPS

Проект можно запускать на Linux VPS через systemd.

Пример unit-файла находится в:

```text
systemd/stream-stats-api.service.example
```

Пример команд:

```bash
sudo cp systemd/stream-stats-api.service.example /etc/systemd/system/stream-stats-api.service
sudo systemctl daemon-reload
sudo systemctl enable stream-stats-api
sudo systemctl start stream-stats-api
sudo systemctl status stream-stats-api
```

## Безопасность

В репозитории не должны храниться:

- реальные API-ключи;
- реальные логины;
- реальные домены, если они относятся к production;
- файл `.env`;
- логи с приватными данными;
- IP-адреса production-сервера.

Все приватные значения передаются через переменные окружения.

## Что реализовано

- REST API на FastAPI
- Pydantic-модели запросов и ответов
- Асинхронные HTTP-запросы через httpx
- Конвертация дат и времени в UTC
- Поддержка часовых поясов через zoneinfo
- Расчёт статистики за выбранный период
- Расчёт бонусного периода между рабочими периодами
- Расчёт итоговой суммы `stream + bonus`
- Очередь запросов через asyncio.Semaphore
- Ограничение конкурентности
- Глобальный rate limit
- Retry-механизм
- Exponential backoff и jitter
- Обработка сетевых ошибок, 429 и 5xx
- Health-check endpoint
- Endpoint для диагностики очереди
- Подготовка сервиса к запуску на VPS через systemd

## Цель проекта

Цель проекта — показать практическую разработку backend-сервиса, который интегрируется с внешним API, контролирует нагрузку, обрабатывает ошибки и предоставляет удобные endpoints для расчёта статистики.
