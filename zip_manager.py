import os
import zipfile
from lock_manager import file_lock
import db
from file_manager import is_safe_path

MAX_EXTRACT_SIZE = 50 * 1024 * 1024  # 50 MB

def create_archive(paths: list[str], zip_path: str, user_id: int, user_dir: str) -> None:

    # Создание ZIP
    # атомарно с file_lock
    # логирование
    # сжатие DEFLATED
    # список путей с поддиректорией

    full_zip = os.path.join(user_dir, zip_path)
    if not is_safe_path(full_zip, user_dir):
        raise ValueError("Обнаружено попытка обхода пути")
    if not zip_path.endswith(".zip"):
        zip_path += ".zip"
        full_zip += ".zip"
    with file_lock:
        file_id = db.get_file_id(zip_path, user_id)
        op_type = "modify" if file_id is not None else "create"
        with zipfile.ZipFile(full_zip, "w", zipfile.ZIP_DEFLATED) as z:
            for p in paths:
                src = os.path.join(user_dir, p)
                if os.path.exists(src):
                    z.write(src, arcname=p)
                else:
                    raise ValueError(f"Путь {p} не найден")
        size = os.path.getsize(full_zip)
        if file_id is not None:
            db.update_file_size(file_id, size)
        else:
            file_id = db.add_file(os.path.basename(zip_path), size, zip_path, user_id)
        db.log_operation(op_type, file_id, user_id)


def extract_zip(zip_path: str, user_id: int, user_dir: str) -> None:
    full_zip = os.path.join(user_dir, zip_path)
    if not is_safe_path(full_zip, user_dir):
        raise ValueError("Обнаружено попытка обхода пути")
    if not os.path.exists(full_zip):
        raise FileNotFoundError("Архив не найден")

    # Директория, куда будем распаковывать — именно та, где лежит архив
    extract_dir = os.path.dirname(full_zip) or user_dir

    # Проверка бомбы
    total_size = 0
    with zipfile.ZipFile(full_zip, "r") as z:
        for info in z.infolist():
            if not info.is_dir():
                total_size += info.file_size
    if total_size > MAX_EXTRACT_SIZE:
        raise ValueError("ZIP-бомба обнаружена")

    # Атомарная распаковка
    with file_lock:
        extracted = []
        with zipfile.ZipFile(full_zip, "r") as z:
            for info in z.infolist():
                member = info.filename.rstrip("/")

                # Защита от traversal
                if member.startswith('/') or member.startswith('\\') or '../' in member or '..\\' in member:
                    raise ValueError("Небезопасный путь в архиве")

                # Путь относительно корня хранилища (для БД)
                rel_path = os.path.join(os.path.dirname(zip_path), member).replace("\\", "/").lstrip("/")
                target = os.path.join(user_dir, rel_path)

                if not is_safe_path(target, user_dir):
                    raise ValueError("Небезопасный путь в архиве")

                # Создаём поддиректории и извлекаем прямо в директорию архива
                os.makedirs(os.path.dirname(target), exist_ok=True)
                z.extract(info, extract_dir)

                if not info.is_dir():
                    f_size = os.path.getsize(target)
                    f_id = db.get_file_id(rel_path, user_id)
                    if f_id is not None:
                        db.update_file_size(f_id, f_size)
                        db.log_operation("modify", f_id, user_id)
                    else:
                        f_id = db.add_file(os.path.basename(rel_path), f_size, rel_path, user_id)
                        db.log_operation("create", f_id, user_id)
                    extracted.append(rel_path)