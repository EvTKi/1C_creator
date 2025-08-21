# main.py
import os
import logging
from pathlib import Path
from modules.config_manager import get_config_manager
from modules.logger_manager import setup_logger
from modules.hierarchy_parser import HierarchyParser
from modules.xml_generator import XMLGenerator
from modules.file_manager import create_cli_manager, create_file_manager


def process_file(csv_path: Path, parent_uid: str):
    logger = setup_logger(log_dir="log", csv_filename=csv_path.name)
    logger.info("=== ЗАПУСК ГЕНЕРАЦИИ RDF/XML ===")
    logger.info(f"Обрабатывается файл: {csv_path}")
    logger.info(f"Родительский UID: {parent_uid}")

    parser = HierarchyParser(str(csv_path))
    try:
        paths, external_children, cck_map, parent_uid_map = parser.parse()

        # Отладочный вывод
        logger.info(f"Пути для создания: {len(paths)}")
        for path in paths:
            logger.debug(f"Создать: {' -> '.join(path)}")

        logger.info(
            f"Виртуальные контейнеры (path_to_uid): {len(parser.path_to_uid)}")
        for path, uid in parser.path_to_uid.items():
            logger.debug(f"Виртуальный: {' -> '.join(path)} -> {uid}")

        logger.info(f"Parent UID map: {parent_uid_map}")

        logger.info(f"Загружено путей: {len(paths)}")
        logger.info(
            f"Внешних ChildObjects: {sum(len(v) for v in external_children.values())}")
        logger.info(f"Детей с виртуальным родителем: {len(parent_uid_map)}")
    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}", exc_info=True)
        return

    if not paths:
        logger.error("Нет данных для обработки")
        return

    generator = XMLGenerator()
    try:
        xml_content = generator.generate(
            paths=paths,
            external_children=external_children,
            parent_uid=parent_uid,
            cck_map=cck_map,
            parent_uid_map=parent_uid_map,
            virtual_containers=set(parser.path_to_uid.keys())
        )
        logger.info("Генерация XML успешна")
    except Exception as e:
        logger.error(f"Ошибка генерации: {e}", exc_info=True)
        return

    output_path = csv_path.with_suffix(".xml")
    try:
        # Удаляем файл если он существует
        if output_path.exists():
            output_path.unlink()

        # Записываем файл
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        logger.info(f"Файл сохранён: {output_path}")
        print(f"✅ {csv_path.name} → {output_path.name}")

        # Добавляем отладку
        logger.debug(
            f"Размер сгенерированного XML: {len(xml_content)} символов")

    except Exception as e:
        logger.error(f"Ошибка сохранения: {e}", exc_info=True)
        return


def main():
    cli_manager = create_cli_manager()
    folder_uid, csv_dir = cli_manager.get_cli_parameters()

    if not folder_uid:
        print("❌ Не указан UID папки.")
        return

    file_manager = create_file_manager(csv_dir)
    if not file_manager.validate_directory():
        print(f"❌ Папка не найдена: {csv_dir}")
        return

    csv_files = file_manager.get_csv_files()
    if not csv_files:
        print("❌ Нет подходящих CSV-файлов.")
        return

    print("Будут обработаны:")
    for f in csv_files:
        print(f"  {f}")
    print("-" * 30)

    # Создаем директорию логов если не существует
    os.makedirs("log", exist_ok=True)

    for filename in csv_files:
        csv_path = file_manager.base_directory / filename
        process_file(csv_path, folder_uid)

    cli_manager.print_completion_message()


if __name__ == "__main__":
    main()
