# Oregon Booking Service

Микросервис для создания и управления бронированиями ресурсов

Поддерживает публикацию ивентов в Kafka (Sarama) для нотификаций

## Быстрый старт

Запуск :

```bash
docker compose up -d
```

## Список ручек

### `BookingService`

- `CreateBooking`
- `GetBooking`
- `UserCancelBooking`
- `AdminCancelBooking`
- `ListBookingsByUser`
- `ListBookingsByResource`

## Kafka события

Если `kafka.enabled=true` в конфиге, то сервис публикует ивенты:

Топики:

- `topic.user.booking` при `CreateBooking`
- `topic.admin.cancel` при `AdminCancelBooking`
- `topic.user.cancel` при `UserCancelBooking`
- `topic.messages.start` для напоминаний за 15 минут до начала брони
- `topic.messages.end` для напоминаний за 15 минут до конца брони

Формат ивентов:

- `User booking`: `to_user`, `status`, `start_time`, `end_time`, `location`, `type`, `name`
- `Admin cancel`: `to_user`, `status`, `start_time`, `end_time`, `location`, `type`, `name`
- `User cancel`: `to_user`, `start_time`, `end_time`, `location`, `type`, `name`
- `Booking reminder`: `to_user`, `start_time`, `end_time`, `location`, `type`, `name`

## Тесты

Прогон всех тестов:

```bash
go test ./...
```

Покрытие по ключевым пакетам:

```bash
go test ./internal/service -cover
```

## Конфиг

- env - Окружение приложения в режиме локальной разработки
- grpc.port - Порт gRPC сервера сервиса бронирования
- metrics.port - Порт для сбора метрик (Prometheus)
- resource_service.address Адрес для подключения к сервису ресурсов
- tracer.end-point- Endpoint OpenTelemetry коллектора для отправки трассировки
- tracer.insecure: true - Отключена проверка SSL сертификата при подключении к трассировщику
- tracer.sample-ratio: 1.0 - 100% трассировки всех запросов (1.0 = 100%, 0.1 = 10%)
- kafka.enabled - режим Kafka
- kafka.brokers - Адреса Kafka брокеров
- kafka.client_id - Идентификатор клиента Kafka
- kafka.topics.user_booking: "topic.user.booking" - Топик для событий пользовательского бронирования
- kafka.topics.admin_cancel: "topic.admin.cancel" - Топик для отмены бронирования администратором
- kafka.topics.user_cancel: "topic.user.cancel" - Топик для отмены бронирования пользователем
- kafka.topics.remind_start: "topic.messages.start" - Топик для напоминаний о начале бронирования
- kafka.topics.remind_end: "topic.messages.end" - Топик для напоминаний об окончании бронирования
- database.host - Хост БД
- database.port - Порт для подключения к PostgreSQL
- database.user - Пользователь БД
- database.password - Пароль БД
- database.name - Имя базы данных
- database.ssl_mode - SSL режим
