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

Три вида топиков:

- `topic.user.booking` при `CreateBooking`
- `topic.admin.cancel` при `AdminCancelBooking`
- `topic.user.cancel` при `UserCancelBooking`

Формат ивентов:

- `User booking`: `to_user`, `status`, `start_time`, `end_time`, `location`, `type`, `name`
- `Admin cancel`: `to_user`, `status`, `start_time`, `end_time`, `location`, `type`, `name`
- `User cancel`: `to_user`, `start_time`, `end_time`, `location`, `type`, `name`


