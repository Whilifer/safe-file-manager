import bcrypt
import sqlite3
import db

def register_user(username: str, password: str):
    if len(username) < 3:
        raise ValueError("Логин должен содержать минимум 3 символа")
    if len(password) < 6:
        raise ValueError("Пароль должен содержать минимум 6 символов")
    # хэширование пароля
    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    try:
        db.add_user(username, password_hash)
    except sqlite3.IntegrityError:
        raise ValueError("Пользователь с таким логином уже существует")

def login_user(username: str, password: str) -> int:
    user = db.get_user(username)
    if not user:
        raise ValueError("Неверный логин или пароль")
    if not bcrypt.checkpw(password.encode('utf-8'), user[2].encode('utf-8')):
        raise ValueError("Неверный логин или пароль")
    return user[0]