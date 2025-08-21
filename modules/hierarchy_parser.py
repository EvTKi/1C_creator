# modules/hierarchy_parser.py

from typing import List, Tuple, Optional, Dict
from pathlib import Path
import csv
import logging
from collections import defaultdict
from .config_manager import get_config_manager


class HierarchyParser:
    """
    Парсер иерархических данных из файла или списка строк.
    Поддерживает CSV с настраиваемыми заголовками, включая CCK_code.
    """

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = Path(file_path) if file_path else None
        self.logger = logging.getLogger("hierarchy_parser")
        self.config = get_config_manager()

    def _read_lines(self) -> List[Tuple[str, str, str]]:  # (path, uid, cck_code)
        """Читает строки из файла или возвращает тестовые данные."""
        if self.file_path and self.file_path.exists():
            self.logger.info(f"Чтение данных из файла: {self.file_path}")

            # Получаем настройки заголовков из конфига
            csv_headers = self.config.get("csv_headers", {})
            path_header = csv_headers.get("path", "path")
            uid_header = csv_headers.get("uid", "uid")
            cck_header = csv_headers.get("CCK_code")  # Может быть None

            self.logger.debug(
                f"Ожидаемые заголовки: path='{path_header}', uid='{uid_header}', CCK_code='{cck_header}'")

            # Пробуем разные кодировки
            encodings = ['utf-8-sig', 'utf-8', 'cp1251', 'windows-1251']
            data = []
            last_error = None

            for encoding in encodings:
                try:
                    with open(self.file_path, 'r', encoding=encoding) as f:
                        # Определяем разделитель
                        sample = f.read(1024)
                        f.seek(0)
                        delimiter = ';' if ';' in sample else '\t' if '\t' in sample else ','

                        reader = csv.DictReader(f, delimiter=delimiter)

                        # Проверяем необходимые поля
                        if path_header not in reader.fieldnames:
                            raise ValueError(
                                f"В CSV отсутствует поле пути: '{path_header}'")
                        if uid_header not in reader.fieldnames:
                            self.logger.warning(
                                f"В CSV отсутствует поле uid: '{uid_header}' (будет пустым)")

                        self.logger.info(
                            f"Файл успешно прочитан в кодировке: {encoding}")

                        for i, row in enumerate(reader):
                            path = row.get(path_header, '').strip()
                            uid = row.get(uid_header, '').strip()
                            cck_code = row.get(
                                cck_header, '').strip() if cck_header else ""

                            if path:
                                # Всегда 3 элемента
                                data.append((path, uid, cck_code))

                    self.logger.debug(f"Прочитано {len(data)} строк")
                    return data

                except UnicodeDecodeError as e:
                    last_error = e
                    self.logger.debug(
                        f"Не удалось прочитать в кодировке {encoding}: {e}")
                    continue
                except Exception as e:
                    self.logger.error(
                        f"Ошибка чтения файла {self.file_path} в кодировке {encoding}: {e}")
                    raise

            self.logger.error(
                f"Не удалось прочитать файл в известных кодировках: {encodings}")
            raise last_error if last_error else UnicodeDecodeError(
                "Неизвестная ошибка кодировки")

        else:
            self.logger.warning(
                "Файл не указан или не существует. Используются тестовые данные.")
            test_data = [
                ("Иркутские тепловые сети\\", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения ЦОЭО", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения ЦОЭО\\ Здания, сооружения для трансформаторов", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения ЦОЭО\\ Здания, сооружения для трансформаторов\\ ЗДАНИЯ И СООРУЖЕНИЯ", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения ЦОЭО\\ Здания, сооружения для трансформаторов\\ ЗДАНИЯ И СООРУЖЕНИЯ\\ Подстанция трансформаторная РК \"Свердловская\"",
                 "12CC9FCB-D36D-504F-BE1C-87FF16A651DA", "ККС-001"),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения ЦОЭО\\ Здания, сооружения ОРУ", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения ЦОЭО\\ Здания, сооружения ОРУ\\ ЗДАНИЯ И СООРУЖЕНИЯ", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения ЦОЭО\\ Здания, сооружения ОРУ\\ ЗДАНИЯ И СООРУЖЕНИЯ\\ ОРУ", "", ""),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения ЦОЭО\\ Здания, сооружения ОРУ\\ ЗДАНИЯ И СООРУЖЕНИЯ\\ ОРУ\\ ОРУ \"Топка\"",
                 "9C347BC4-B366-5446-90D8-4E061FA257CA", "ККС-002"),
                ("Иркутские тепловые сети\\ Здания и сооружения\\ Здания и сооружения\\ Здания, сооружения для традиционной выработки тепла", "", "ККС-003"),
            ]
            self.logger.debug(
                f"Используются тестовые данные: {len(test_data)} строк")
            return test_data

    def parse(self) -> Tuple[List[Tuple[str, ...]], Dict[Tuple[str, ...], List[str]], Dict[Tuple[str, ...], str]]:
        """
        Парсит данные и возвращает:
        - paths: пути для создания (без объектов с uid)
        - external_children: {parent_path: [uid1, uid2, ...]}
        - cck_map: {path: cck_code}
        """
        self.logger.info(
            "Начало парсинга иерархии с поддержкой внешних uid и CCK")

        lines = self._read_lines()  # Теперь всегда (path, uid, cck_code)

        path_to_uid = {}
        path_to_cck = {}
        all_raw_paths = []

        for line, uid, cck_code in lines:
            parts = tuple(p.strip() for p in line.split('\\') if p.strip())
            if parts:
                all_raw_paths.append(parts)
                if uid:
                    path_to_uid[parts] = uid
                if cck_code:
                    path_to_cck[parts] = cck_code

        paths_to_create = set()
        external_children = defaultdict(list)

        for path in all_raw_paths:
            if path in path_to_uid:
                uid = path_to_uid[path]
                parent_path = path[:-1] if len(path) > 1 else tuple()
                if parent_path:
                    external_children[parent_path].append(uid)
                    paths_to_create.add(parent_path)
                    for i in range(1, len(parent_path)):
                        ancestor = parent_path[:i]
                        paths_to_create.add(ancestor)
                else:
                    self.logger.warning(
                        f"uid '{uid}' для корневого уровня пропущен")
            else:
                paths_to_create.add(path)
                for i in range(1, len(path)):
                    ancestor = path[:i]
                    paths_to_create.add(ancestor)

        paths = list(paths_to_create)
        return paths, dict(external_children), path_to_cck, path_to_uid  # ← 4
