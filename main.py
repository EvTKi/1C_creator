# main.py
"""
Точка входа в приложение с поддержкой внешних uid и CCK-кодов.
"""

import os
import logging
from pathlib import Path
from modules.config_manager import get_config_manager
from modules.logger_manager import LogManager
from modules.hierarchy_parser import HierarchyParser
from modules.xml_generator import XMLGenerator


def main():
    config = get_config_manager()
    file_config = config.config.get("file_management", {})
    log_dir = file_config.get("log_directory", "log")
    input_dir = file_config.get("input_directory", "input")
    output_dir = file_config.get("output_directory", "output")

    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    logger = LogManager.get_logger("main", log_file_path=f"{log_dir}/main.log")
    logger.info("=== ЗАПУСК ПРОГРАММЫ ГЕНЕРАЦИИ RDF/XML ===")
    logger.debug(f"Конфигурация загружена из: {config.config_path}")

    logger.info(f"Директория логов: {log_dir}")
    logger.info(f"Директория входных данных: {input_dir}")
    logger.info(f"Директория выходных данных: {output_dir}")

    print("\n=== Настройка внешнего родителя ===")
    parent_uid = input(
        "Введите UID внешнего родительского объекта (например, #_d69453f3-...): ").strip()
    if not parent_uid.startswith("#"):
        parent_uid = "#" + parent_uid
    logger.info(f"Корень будет привязан к родителю: {parent_uid}")

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
        paths, external_children, cck_map, uid_map = parser.parse()
        logger.info(f"Успешно загружено {len(paths)} путей")
        logger.info(
            f"Внешних uid: {sum(len(v) for v in external_children.values())}")
        logger.info(f"CCK-кодов найдено: {len(cck_map)}")

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("=== CCK-коды ===")
            for path, code in list(cck_map.items())[:5]:
                logger.debug(f"  {'\\'.join(path)} -> {code}")

    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}", exc_info=True)
        return

    if not paths:
        logger.error("Нет данных для обработки")
        return

    logger.info("Начинаем генерацию RDF/XML...")
    generator = XMLGenerator()
    try:
        xml_content = generator.generate(
            paths,
            external_children,
            parent_uid=parent_uid,
            cck_map=cck_map,
            uid_map=uid_map  # ✅ Передаём uid_map
        )
        logger.info("Генерация XML завершена успешно")
    except Exception as e:
        logger.error(f"Ошибка генерации XML: {e}", exc_info=True)
        return

    output_path = Path(output_dir) / "output.xml"
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        logger.info(f"Файл успешно сохранён: {output_path}")
        print(f"✅ Генерация завершена: {output_path}")
    except Exception as e:
        logger.error(f"Ошибка сохранения файла: {e}", exc_info=True)


if __name__ == "__main__":
    main()


"""
теперь необходимо провести анализ следующего случая:
Если у объекта имеется собственный uid, и в дальнейшем у него могут встречаться дочерние объекты, то необходимо дочерним объектам в качестве parent указывать собственный uid объекта - родителя"""
