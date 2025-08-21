# main.py
"""
Точка входа в приложение: пакетная обработка всех CSV-файлов.
"""

import os
import logging
from pathlib import Path
from modules.config_manager import get_config_manager
from modules.logger_manager import LogManager, setup_logger
from modules.hierarchy_parser import HierarchyParser
from modules.xml_generator import XMLGenerator
from modules.file_manager import create_file_manager, create_cli_manager


def process_csv_file(csv_path: Path, parent_uid: str, file_manager, logger):
    """Обрабатывает один CSV-файл."""
    logger.info(f"=== НАЧАЛО ОБРАБОТКИ: {csv_path.name} ===")

    # Парсинг
    parser = HierarchyParser(str(csv_path))
    try:
        paths, external_children, cck_map, uid_map = parser.parse()
        logger.info(
            f"Загружено: {len(paths)} путей, {len(external_children)} внешних uid")
    except Exception as e:
        logger.error(f"Ошибка парсинга {csv_path.name}: {e}", exc_info=True)
        return False

    if not paths:
        logger.warning("Нет данных для обработки")
        return False

    # Генерация XML
    generator = XMLGenerator()
    try:
        xml_content = generator.generate(
            paths=paths,
            external_children=external_children,
            parent_uid=parent_uid,
            cck_map=cck_map,
            uid_map=uid_map
        )
        logger.info("Генерация XML завершена")
    except Exception as e:
        logger.error(f"Ошибка генерации XML: {e}", exc_info=True)
        return False

    # Сохранение
    output_path = csv_path.with_suffix(".xml")
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        logger.info(f"Файл сохранён: {output_path}")
        print(f"✅ {csv_path.name} → {output_path.name}")
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения {output_path}: {e}", exc_info=True)
        return False


def main():
    # === Инициализация конфигурации ===
    config = get_config_manager()
    file_config = config.config.get("file_management", {})
    log_dir = file_config.get("log_directory", "log")
    exclude_files = file_config.get("exclude_files", ["Sample.csv"])

    # Создаём менеджеры
    cli_manager = create_cli_manager()
    folder_uid, csv_dir = cli_manager.get_cli_parameters()

    if not folder_uid:
        print("❌ Не указан UID папки.")
        return

    # Подготавливаем директории
    file_manager = create_file_manager(csv_dir)
    os.makedirs(log_dir, exist_ok=True)

    # Валидация и получение файлов
    csv_files = cli_manager.validate_and_list_files(file_manager)
    if not csv_files:
        return

    # === Обработка каждого файла ===
    for csv_filename in csv_files:
        csv_path = file_manager.base_directory / csv_filename

        # Настройка логгера для текущего файла
        logger = setup_logger(log_dir=log_dir, csv_filename=csv_filename)

        logger.info("=== ЗАПУСК ПРОГРАММЫ ГЕНЕРАЦИИ RDF/XML ===")
        logger.info(f"Обрабатывается файл: {csv_path}")
        logger.info(f"Конфигурация: {config.config_path}")
        logger.info(f"Родительский UID: {folder_uid}")

        # Обработка
        success = process_csv_file(csv_path, folder_uid, file_manager, logger)
        if not success:
            print(f"❌ Ошибка при обработке {csv_filename}")

    # Финальное сообщение
    cli_manager.print_completion_message()


if __name__ == "__main__":
    main()


"""
теперь необходимо провести анализ следующего случая:
Если у объекта имеется собственный uid, и в дальнейшем у него могут встречаться дочерние объекты, то необходимо дочерним объектам в качестве parent указывать собственный uid объекта - родителя"""
