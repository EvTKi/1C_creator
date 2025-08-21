# modules/hierarchy_parser.py
"""
Парсер иерархии с поддержкой внешних uid как виртуальных контейнеров.
Объект с uid НЕ создаётся, но его uid используется как ParentObject для детей.
"""

from typing import List, Tuple, Optional, Dict
from pathlib import Path
import csv
import logging
from collections import defaultdict
from .config_manager import get_config_manager


class HierarchyParser:
    """
    Парсер иерархических данных из CSV.
    Объекты с uid не создаются, но их uid используется как родитель для детей.
    """

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = Path(file_path) if file_path else None
        self.logger = logging.getLogger("hierarchy_parser")
        self.config = get_config_manager()
        self.paths_with_uid = set()

    def _read_lines(self) -> List[Tuple[str, str, str]]:
        """Читает строки из файла или возвращает тестовые данные."""
        if self.file_path and self.file_path.exists():
            self.logger.info(f"Чтение данных из файла: {self.file_path}")

            # Получаем настройки заголовков
            csv_headers = self.config.get("csv_headers", {})
            path_header = csv_headers.get("path", "path")
            uid_header = csv_headers.get("uid", "uid")
            cck_header = csv_headers.get("CCK_code")

            # Пробуем разные кодировки
            encodings = ['utf-8-sig', 'utf-8', 'cp1251', 'windows-1251']
            data = []
            last_error = None

            for encoding in encodings:
                try:
                    with open(self.file_path, 'r', encoding=encoding) as f:
                        sample = f.read(1024)
                        f.seek(0)
                        delimiter = ';' if ';' in sample else '\t' if '\t' in sample else ','

                        reader = csv.DictReader(f, delimiter=delimiter)

                        if path_header not in reader.fieldnames:
                            raise ValueError(
                                f"В CSV отсутствует поле пути: '{path_header}'")

                        for i, row in enumerate(reader):
                            path = row.get(path_header, '').strip()
                            uid = row.get(uid_header, '').strip()
                            cck_code = row.get(
                                cck_header, '').strip() if cck_header else ""

                            if path:
                                data.append((path, uid, cck_code))

                    self.logger.debug(f"Прочитано {len(data)} строк")
                    return data

                except UnicodeDecodeError as e:
                    last_error = e
                    self.logger.debug(
                        f"Не удалось прочитать в кодировке {encoding}: {e}")
                    continue
                except Exception as e:
                    self.logger.error(f"Ошибка чтения файла: {e}")
                    raise

            self.logger.error(
                f"Не удалось прочитать файл в известных кодировках: {encodings}")
            raise last_error

        else:
            self.logger.warning(
                "Файл не найден. Используются тестовые данные.")
            test_data = [
                ("A\\", "", ""),
                ("A\\B", "123-456", ""),
                ("A\\B\\C", "", ""),
                ("A\\B\\C\\D", "", ""),
            ]
            return test_data

    def parse(self) -> Tuple[
        List[Tuple[str, ...]],  # paths для создания
        Dict[Tuple[str, ...], List[str]],  # external_children: parent → [uid]
        Dict[Tuple[str, ...], str],  # cck_map
        Dict[Tuple[str, ...], str]  # parent_uid_map: child_path → parent_uid
    ]:
        lines = self._read_lines()

        # Собираем данные
        path_to_uid = {}  # пути → uid
        path_to_cck = {}
        all_paths = []    # все пути из CSV

        for line, uid, cck_code in lines:
            parts = tuple(p.strip() for p in line.split('\\') if p.strip())
            if parts:
                all_paths.append(parts)
                if uid:
                    path_to_uid[parts] = uid
                if cck_code:
                    path_to_cck[parts] = cck_code

        # Сохраняем как атрибут для доступа извне
        self.path_to_uid = path_to_uid  # Добавить эту строку!

        # === Определяем, что создавать ===
        paths_to_create = set()      # объекты для создания в XML
        external_children = defaultdict(list)  # родитель → [uid] внешних детей
        parent_uid_map = {}          # ребенок → uid виртуального родителя

        # 1. Создаем карту замен: путь с UID → его UID
        path_replacements = {}
        for path, uid in path_to_uid.items():
            path_replacements[path] = uid

        # 2. Добавляем пути для создания (исключая виртуальные контейнеры)
        for path in all_paths:
            if path not in path_to_uid:  # Не виртуальный контейнер
                paths_to_create.add(path)

        # 3. Добавляем предков, но с заменой виртуальных контейнеров
        for path in list(paths_to_create):
            # Для каждого пути, заменяем виртуальные контейнеры на их UID
            normalized_path = []
            for i in range(1, len(path) + 1):
                segment = path[:i]
                # Если сегмент является виртуальным контейнером, заменяем на его UID
                if segment in path_to_uid:
                    # Не добавляем сегмент, он виртуальный
                    continue
                else:
                    normalized_path.append(segment)

            # Добавляем все нормализованные сегменты
            for norm_seg in normalized_path:
                if norm_seg not in paths_to_create:
                    paths_to_create.add(norm_seg)

        # 4. Обрабатываем виртуальные контейнеры
        for virtual_path, uid in path_to_uid.items():
            # Виртуальный контейнер добавляется как внешний ребенок своему родителю
            if len(virtual_path) > 1:
                parent_path = virtual_path[:-1]
                # Если родитель не виртуальный, добавляем внешнего ребенка
                if parent_path not in path_to_uid:
                    external_children[parent_path].append(uid)

            # Дети виртуального контейнера получают ссылку на его uid как родителя
            for child_path in all_paths:
                if (len(child_path) == len(virtual_path) + 1 and
                        child_path[:len(virtual_path)] == virtual_path):
                    parent_uid_map[child_path] = uid

        return (
            sorted(list(paths_to_create)),
            dict(external_children),
            path_to_cck,
            parent_uid_map
        )
