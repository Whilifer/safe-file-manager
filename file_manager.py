import os
import shutil
import asyncio
from lock_manager import file_lock
import db

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB

def is_safe_path(path: str, base_dir: str) -> bool:
    """
    # от Path Traversal:
    проверка что путь не выходит за base_dir включая абсолютные пути)
    """
    try:
        base = os.path.abspath(base_dir) + os.sep
        full = os.path.abspath(path)
        return full.startswith(base)
    except (ValueError, OSError):
        return False

def write_file(path: str, content: bytes | str, user_id: int, user_dir: str, mode: str = 'w') -> None:
    # безопасная запись и модификация файла
    # проверка пути
    # проверка размера
    # атомарно: бд и диск под file_lock
    # логирование

    full_path = os.path.join(user_dir, path)
    if not is_safe_path(full_path, user_dir):
        raise ValueError("Обнаружено попытка обхода пути (path traversal)")
    if isinstance(content, str):
        content = content.encode("utf-8")
    size = len(content)
    if size > MAX_FILE_SIZE:
        raise ValueError(f"Размер файла превышает лимит {MAX_FILE_SIZE // (1024*1024)} MB")
    open_mode = 'ab' if mode == 'a' else 'wb'
    with file_lock:
        file_id = db.get_file_id(path, user_id)
        if file_id is not None:
            op_type = "modify"
            if mode == 'a':
                size += os.path.getsize(full_path) if os.path.exists(full_path) else 0
            db.update_file_size(file_id, size)
        else:
            op_type = "create"
            file_id = db.add_file(os.path.basename(path), size, path, user_id)
        # Запись на диск
        with open(full_path, open_mode) as f:
            f.write(content)
        db.log_operation(op_type, file_id, user_id)

async def async_write_file(path: str, content: bytes | str, user_id: int, user_dir: str, mode: str = 'w') -> None:
    # асинхронный write_file
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, write_file, path, content, user_id, user_dir, mode)

def read_file(path: str, user_id: int, user_dir: str, offset: int = 0, count: int = None) -> bytes | str:
    # безопасное чтение файла c роверкой пути и доступа через бд и file_lock
    # offset и count
    # на возврат bytes или decode to str если текст

    full_path = os.path.join(user_dir, path)
    if not is_safe_path(full_path, user_dir):
        raise ValueError("Обнаружено попытка обхода пути")
    with file_lock:
        file_id = db.get_file_id(path, user_id)
        if file_id is None:
            raise FileNotFoundError("Файл не найден или нет доступа. Убедитесь в правильном имени (с расширением).")
        with open(full_path, "rb") as f:
            f.seek(offset)
            data = f.read(count) if count else f.read()
        return data

async def async_read_file(path: str, user_id: int, user_dir: str, offset: int = 0, count: int = None) -> bytes | str:
    # асинхронный read_file
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, read_file, path, user_id, user_dir, offset, count)


def delete_file(path: str, user_id: int, user_dir: str) -> None:
    full_path = os.path.join(user_dir, path)
    if not is_safe_path(full_path, user_dir):
        raise ValueError("Обнаружено попытка обхода пути")
    with file_lock:
        file_id = db.get_file_id(path, user_id)
        if file_id is None:
            raise FileNotFoundError("Файл не найден или нет доступа")

        # Сначала логируем удаление (пока file_id ещё есть)
        db.log_operation("delete", file_id, user_id)

        # Удаляем с диска (если файл уже удалён — просто игнорируем)
        try:
            os.remove(full_path)
        except FileNotFoundError:
            pass  # файл уже отсутствует — ничего страшного

        # Удаляем запись из Files (Operations.file_id станет NULL автоматически)
        db.delete_file_record(file_id)

def copy_file(src_path: str, dest_path: str, user_id: int, user_dir: str) -> None:
    # те же меры безопасности
    full_src = os.path.join(user_dir, src_path)
    full_dest = os.path.join(user_dir, dest_path)
    if not (is_safe_path(full_src, user_dir) and is_safe_path(full_dest, user_dir)):
        raise ValueError("Обнаружено попытка обхода пути")
    with file_lock:
        file_id = db.get_file_id(src_path, user_id)
        if file_id is None:
            raise FileNotFoundError("Источник не найден")
        shutil.copy(full_src, full_dest)
        size = os.path.getsize(full_dest)
        new_file_id = db.add_file(os.path.basename(dest_path), size, dest_path, user_id)
        db.log_operation("create", new_file_id, user_id)

def move_file(src_path: str, dest_path: str, user_id: int, user_dir: str) -> None:
    full_src = os.path.join(user_dir, src_path)
    full_dest = os.path.join(user_dir, dest_path)
    if not (is_safe_path(full_src, user_dir) and is_safe_path(full_dest, user_dir)):
        raise ValueError("Обнаружено попытка обхода пути")
    with file_lock:
        file_id = db.get_file_id(src_path, user_id)
        if file_id is None:
            raise FileNotFoundError("Источник не найден")
        shutil.move(full_src, full_dest)
        db.update_file_location(file_id, dest_path)   # теперь точно обновляется
        db.log_operation("modify", file_id, user_id)

def create_directory(subdir: str, user_id: int, user_dir: str) -> None:
    # те же меры безопасности
    full_path = os.path.join(user_dir, subdir)
    if not is_safe_path(full_path, user_dir):
        raise ValueError("Обнаружено попытка обхода пути")
    with file_lock:
        os.makedirs(full_path, exist_ok=True)
        db.log_operation("dir_create", None, user_id)

def delete_directory(subdir: str, user_id: int, user_dir: str, recursive: bool = False) -> None:
    full_path = os.path.join(user_dir, subdir)
    if not is_safe_path(full_path, user_dir):
        raise ValueError("Обнаружено попытка обхода пути")
    with file_lock:
        if recursive:
            shutil.rmtree(full_path)   # удаляем всё с диска
            # ← КАСКАДНОЕ УДАЛЕНИЕ ИЗ БД
            with db.get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM Files WHERE location LIKE ? AND owner_id = ?",
                            (f"{subdir}/%", user_id))
                cur.execute("DELETE FROM Files WHERE location = ? AND owner_id = ?",
                            (subdir, user_id))  # на случай если есть запись с точным путём
        else:
            os.rmdir(full_path)

        db.log_operation("dir_delete", None, user_id)


def move_directory(src_subdir: str, dest_subdir: str, user_id: int, user_dir: str) -> None:
    full_src = os.path.join(user_dir, src_subdir)
    full_dest = os.path.join(user_dir, dest_subdir)
    if not (is_safe_path(full_src, user_dir) and is_safe_path(full_dest, user_dir)):
        raise ValueError("Обнаружено попытка обхода пути")

    # Проверяем, что источник — действительно директория
    if not os.path.isdir(full_src):
        raise ValueError("Источник не является директорией")

    with file_lock:
        shutil.move(full_src, full_dest)

        # Правильное обновление путей в БД
        with db.get_db_connection() as conn:
            cur = conn.cursor()
            # Обновляем все файлы внутри перемещённой директории
            cur.execute("""
                UPDATE Files 
                SET location = REPLACE(location, ?, ?)
                WHERE location LIKE ? AND owner_id = ?
            """, (src_subdir + '/', dest_subdir + '/', src_subdir + '/%', user_id))
            # Обновляем запись самой директории (если она была как файл с путём src_subdir)
            cur.execute("""
                UPDATE Files 
                SET location = ? 
                WHERE location = ? AND owner_id = ?
            """, (dest_subdir, src_subdir, user_id))

        db.log_operation("dir_move", None, user_id)

def list_directory(subdir: str, user_id: int, user_dir: str) -> list:
    # те же меры безопасности
    full_path = os.path.join(user_dir, subdir)
    if not is_safe_path(full_path, user_dir):
        raise ValueError("Обнаружено попытка обхода пути")
    with file_lock:
        return os.listdir(full_path)