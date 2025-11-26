from threading import Lock

# глобальная блокировка для атомарного доступа к файлам и бд. защита от race conditions
file_lock = Lock()