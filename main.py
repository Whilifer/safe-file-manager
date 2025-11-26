import os
import shutil
import psutil
import asyncio
import db
import auth
from file_manager import (
    write_file, read_file, delete_file, copy_file, move_file,
    create_directory, delete_directory, move_directory, list_directory, async_write_file, async_read_file
)
from json_xml_handler import write_json, read_json, write_xml, read_xml, edit_xml_add_element
from zip_manager import create_archive, extract_zip

BASE_DIR = "./storage"

async def main():
    os.makedirs(BASE_DIR, exist_ok=True)
    db.init_db()

    logged_in = False
    current_user_id = None
    user_dir = None
    current_username = None

    while True:
        if not logged_in:
            print("          БЕЗОПАСНЫЙ ФАЙЛОВЫЙ МЕНЕДЖЕР")
            print("1. Регистрация")
            print("2. Вход")
            print("0. Выход")
            choice = input("\nВыберите действие: ").strip()

            if choice == "1":
                username = input("Логин (мин. 3 символа): ").strip()
                password = input("Пароль (мин. 6 символов): ").strip()
                try:
                    auth.register_user(username, password)
                    print("Пользователь успешно зарегистрирован.")
                except ValueError as e:
                    print(f"Ошибка: {e}")

            elif choice == "2":
                username = input("Логин: ").strip()
                password = input("Пароль: ").strip()
                try:
                    user_id = auth.login_user(username, password)
                    current_user_id = user_id
                    current_username = username
                    user_dir = os.path.join(BASE_DIR, username)
                    os.makedirs(user_dir, exist_ok=True)
                    logged_in = True
                    print(f"Успешный вход: {username}")
                except ValueError as e:
                    print(f"Ошибка: {e}")

            elif choice == "0":
                print("Выключение")
                break
            continue

        # Расширенное меню
        print(f"           МЕНЮ ПОЛЬЗОВАТЕЛЯ: {current_username}")
        print("1. Создать или изменить файл")
        print("2. Прочитать файл")
        print("3. Удалить файл")
        print("4. Записать JSON или XML")
        print("5. Прочитать JSON или XML")
        print("6. Архивировать файлы")
        print("7. Разархивировать ZIP")
        print("8. Список файлов")
        print("9. Информация о дисках и хранилище")
        print("10. Создать директорию")
        print("11. Удалить директорию")
        print("12. Переместить директорию")
        print("13. Список содержимого директории")
        print("14. Копировать файл")
        print("15. Переместить файл")
        print("16. Добавить элемент в XML")
        print("17. Выход")
        choice = input("\nВыберите действие: ").strip()

        if choice == "1":
            path = input("Путь к файлу (e.g., subdir/test.txt): ").strip()
            content = input("Содержимое: ").strip()
            mode = input("Mode (w - overwrite, a - append): ").strip() or "w"
            try:
                await async_write_file(path, content, current_user_id, user_dir, mode)
                print("Успех: Файл создан или обновлён и залогирован.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "2":
            path = input("Путь к файлу: ").strip()
            offset = int(input("Offset (0): ").strip() or 0)
            count = int(input("Count (all): ").strip() or 0) or None
            try:
                content = await async_read_file(path, current_user_id, user_dir, offset, count)
                if isinstance(content, bytes):
                    content = content.decode("utf-8", errors="replace")
                print(f"\n📄 {path}:\n{content}\n")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "3":
            path = input("Путь к файлу: ").strip()
            try:
                delete_file(path, current_user_id, user_dir)
                print("Файл успешно удалён и залогирован.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "4":
            data_type = input("Тип (j - JSON, x - XML): ").strip().lower()
            path = input("Путь к файлу: ").strip()
            data_input = input("Данные: ").strip()
            try:
                if data_type == "j":
                    ignore_null = input("Ignore null (y/n): ").strip().lower() == 'y'
                    write_indented = input("Indented (y/n): ").strip().lower() != 'n'
                    write_json(path, data_input, current_user_id, user_dir, ignore_null, write_indented)
                elif data_type == "x":
                    write_xml(path, data_input, current_user_id, user_dir)
                else:
                    raise ValueError("Используйте j или x")
                print("JSON или XML записан.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "5":
            data_type = input("Тип (j - JSON, x - XML): ").strip().lower()
            path = input("Путь к файлу: ").strip()
            try:
                if data_type == "j":
                    pretty = read_json(path, current_user_id, user_dir)
                elif data_type == "x":
                    pretty = read_xml(path, current_user_id, user_dir)
                else:
                    raise ValueError("Используйте j или x")
                print(f"\n{path}:\n{pretty}\n")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "6":
            paths_str = input("Пути для архивирования (через ,): ").strip()
            paths = [p.strip() for p in paths_str.split(',')]
            zip_path = input("Имя архива (по умолчанию archive.zip): ").strip() or "archive.zip"
            try:
                create_archive(paths, zip_path, current_user_id, user_dir)
                print("Архив создан.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "7":
            zip_path = input("Путь к архиву: ").strip()
            try:
                extract_zip(zip_path, current_user_id, user_dir)
                print("ZIP разархивирован.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "8":
            files = db.get_user_files(current_user_id)
            if not files:
                print("У вас нет файлов")
            else:
                print("Ваши файлы:")
                print("-" * 50)
                for fn, sz, created, loc in files:
                    print(f"{loc:<30} {sz:>12,} байт | {created}")
            input("\nНажмите Enter для продолжения...")

        elif choice == "9":
            # инфо о дисках
            files = db.get_user_files(current_user_id)
            total_bytes = sum(sz for _, sz, _, _ in files)
            if total_bytes < 1024:
                user_size = f"{total_bytes} байт"
            elif total_bytes < 1024 * 1024:
                user_size = f"{total_bytes / 1024:.2f} КБ"
            else:
                user_size = f"{total_bytes / (1024 * 1024):.2f} МБ"
            print(f"Размер ваших файлов: {user_size}")
            print("Диски:")
            for part in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(part.mountpoint)
                    print(f"  {part.device} ({part.fstype}): Всего {usage.total / (1024**3):.1f} GB, Свободно {usage.free / (1024**3):.1f} GB, Тип: {part.opts}")
                except Exception:
                    print(f"  {part.device}: Недоступен")
            input("\nНажмите Enter для продолжения...")

        elif choice == "10":
            subdir = input("Путь к директории: ").strip()
            try:
                create_directory(subdir, current_user_id, user_dir)
                print("Директория создана.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "11":
            subdir = input("Путь к директории: ").strip()
            recursive = input("Рекурсивно (y/n): ").strip().lower() == 'y'
            try:
                delete_directory(subdir, current_user_id, user_dir, recursive)
                print("Директория удалена.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "12":
            src = input("Источник директории: ").strip()
            dest = input("Цель: ").strip()
            try:
                move_directory(src, dest, current_user_id, user_dir)
                print("Директория перемещена.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "13":
            subdir = input("Путь к директории (пусто для root): ").strip()
            try:
                contents = list_directory(subdir, current_user_id, user_dir)
                print(f"Содержимое {subdir or '/'}:\n{', '.join(contents)}")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "14":
            src = input("Источник файла: ").strip()
            dest = input("Цель: ").strip()
            try:
                copy_file(src, dest, current_user_id, user_dir)
                print("Файл скопирован.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "15":
            src = input("Источник файла: ").strip()
            dest = input("Цель: ").strip()
            try:
                move_file(src, dest, current_user_id, user_dir)
                print("Файл перемещён.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "16":
            path = input("Путь к XML: ").strip()
            xpath = input("XPath для parent: ").strip()
            elem_name = input("Имя нового элемента: ").strip()
            value = input("Значение: ").strip()
            try:
                edit_xml_add_element(path, xpath, elem_name, value, current_user_id, user_dir)
                print("Элемент добавлен в XML.")
            except Exception as e:
                print(f"Ошибка: {e}")

        elif choice == "17":
            print("Выход из аккаунта")
            logged_in = False
            current_user_id = None
            user_dir = None
            current_username = None

        else:
            print("Неверный выбор")

if __name__ == "__main__":
    asyncio.run(main())