import sqlite3
from contextlib import contextmanager

DB_PATH = "file_manager.db"

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_db_connection() as conn:
        cur = conn.cursor()
        # пользователи
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )
        """)
        # файлы
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                size INTEGER,
                location TEXT,
                owner_id INTEGER,
                FOREIGN KEY (owner_id) REFERENCES Users(id)
            )
        """)
        # операции
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                operation_type TEXT CHECK(operation_type IN ('create', 'modify', 'delete', 'dir_create', 'dir_delete', 'dir_move')),
                file_id INTEGER,   -- nullable
                user_id INTEGER,
                FOREIGN KEY (file_id) REFERENCES Files(id) ON DELETE SET NULL,  -- ← главное изменение
                FOREIGN KEY (user_id) REFERENCES Users(id)
            )
        """)

def add_user(username: str, password_hash: str):
    # prepared statement
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO Users (username, password_hash) VALUES (?, ?)",
            (username, password_hash)
        )

def get_user(username: str):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, username, password_hash FROM Users WHERE username = ?",
            (username,)
        )
        row = cur.fetchone()
        return row

def add_file(filename: str, size: int, location: str, owner_id: int) -> int:
    # запись о файле prepared, возвращает id
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO Files (filename, size, location, owner_id) VALUES (?, ?, ?, ?)",
            (filename, size, location, owner_id)
        )
        return cur.lastrowid

def get_file_id(location: str, owner_id: int) -> int | None:
    # получение id файла по пути и владельцу с проверкой доступа
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM Files WHERE location = ? AND owner_id = ?",
            (location, owner_id)
        )
        row = cur.fetchone()
        return row[0] if row else None

def update_file_size(file_id: int, size: int):
    # обновление размера файла prepared
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE Files SET size = ? WHERE id = ?",
            (size, file_id)
        )

def update_file_location(file_id: int, new_location: str):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE Files SET location = ? WHERE id = ?",
            (new_location, file_id)
        )

def delete_file_record(file_id: int):
    # удаление записи о файле prepared, cascade удалит operations
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM Files WHERE id = ?",
            (file_id,)
        )

def log_operation(operation_type: str, file_id: int | None, user_id: int):
    # логирование операции в бд prepared, file_id
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO Operations (operation_type, file_id, user_id) VALUES (?, ?, ?)",
            (operation_type, file_id, user_id)
        )

def get_user_files(owner_id: int):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT filename, size, created_at, location FROM Files WHERE owner_id = ? ORDER BY created_at DESC",
            (owner_id,)
        )
        return cur.fetchall()