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
