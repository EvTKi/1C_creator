# main.py
"""
Точка входа в приложение с поддержкой внешних uid и подробным логированием.
"""

import os
import logging
from pathlib import Path
from modules.config_manager import get_config_manager
from modules.logger_manager import LogManager
from modules.hierarchy_parser import HierarchyParser
from modules.xml_generator import XMLGenerator


def main():
    # === Инициализация логирования ===
    config = get_config_manager()
    file_config = config.config.get("file_management", {})
    log_dir = file_config.get("log_directory", "log")
    input_dir = file_config.get("input_directory", "input")
    output_dir = file_config.get("output_directory", "output")

    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Настройка логгера
    logger = LogManager.get_logger("main", log_file_path=f"{log_dir}/main.log")
    logger.info("=== ЗАПУСК ПРОГРАММЫ ГЕНЕРАЦИИ RDF/XML ===")
    logger.debug(f"Конфигурация загружена из: {config.config_path}")

    # === Настройка директорий ===
    logger.info(f"Директория логов: {log_dir}")
    logger.info(f"Директория входных данных: {input_dir}")
    logger.info(f"Директория выходных данных: {output_dir}")

    # === Ввод parent_uid ===
    print("\n=== Настройка внешнего родителя ===")
    parent_uid = input(
        "Введите UID внешнего родительского объекта (например, #_d69453f3-...): ").strip()
    if not parent_uid.startswith("#"):
        parent_uid = "#" + parent_uid
    logger.info(f"Корень будет привязан к родителю: {parent_uid}")

    # === Парсинг иерархии ===
    logger.info("Начинаем загрузку иерархических данных...")
    input_path = Path(input_dir) / "input.csv"
    if not input_path.exists():
        logger.warning(
            f"Файл {input_path} не найден. Используются тестовые данные.")
        input_path = None
    else:
        logger.info(f"Загружаем данные из файла: {input_path}")

    parser = HierarchyParser(str(input_path) if input_path else None)
    try:
        paths, external_children = parser.parse()
        logger.info(f"Успешно загружено {len(paths)} путей")
        logger.info(
            f"Внешних uid: {sum(len(v) for v in external_children.values())}")

        # Детальное логирование для отладки
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("=== Детали парсинга ===")
            logger.debug(f"Пути для создания ({len(paths)}):")
            for i, path in enumerate(paths[:10]):  # Первые 10
                logger.debug(f"  {i+1}. {'\\'.join(path)}")
            if len(paths) > 10:
                logger.debug(f"  ... и ещё {len(paths) - 10} путей")

            logger.debug(f"Внешние дети ({len(external_children)} родителей):")
            # Первые 5
            for i, (parent, uids) in enumerate(list(external_children.items())[:5]):
                logger.debug(f"  {i+1}. {'\\'.join(parent)} -> {uids}")
            if len(external_children) > 5:
                logger.debug(
                    f"  ... и ещё {len(external_children) - 5} родителей")

    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}", exc_info=True)
        return

    if not paths:
        logger.error("Нет данных для обработки")
        return

    # === Генерация XML ===
    logger.info("Начинаем генерацию RDF/XML...")
    generator = XMLGenerator()
    try:
        xml_content = generator.generate(
            paths, external_children, parent_uid=parent_uid)
        logger.info("Генерация XML завершена успешно")
    except Exception as e:
        logger.error(f"Ошибка генерации XML: {e}", exc_info=True)
        return

    # === Сохранение результата ===
    output_path = Path(output_dir) / "output.xml"
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        logger.info(f"Файл успешно сохранён: {output_path}")
        print(f"✅ Генерация завершена: {output_path}")

        # Дополнительная информация
        total_objects = len(paths)
        total_external_uids = sum(len(v) for v in external_children.values())
        logger.info(
            f"Статистика: {total_objects} объектов, {total_external_uids} внешних ссылок")

    except Exception as e:
        logger.error(f"Ошибка сохранения файла: {e}", exc_info=True)


if __name__ == "__main__":
    main()
