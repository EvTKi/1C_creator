"""
Модуль генерации RDF/XML по иерархическим данным.

Ответственность:
- Построение XML на основе путей, UID, ККС и виртуальных контейнеров
- Поддержка CIM16, включая AssetContainer и GenericPSR
- Генерация UUID, управление иерархией и ссылками

Новые правила для ККС:
- AssetContainer: <me:IdentifiedObject.mRIDStr>{ККС}</me:IdentifiedObject.mRIDStr>
- GenericPSR: <rh:PowerSystemResource.ccsCode>{ККС}</rh:PowerSystemResource.ccsCode>
"""

from typing import List, Tuple, Dict, Set
from uuid import uuid4
import logging
from .config_manager import get_config_manager
from collections import defaultdict
from queue import Queue


class XMLGenerator:
    """
    Генератор RDF/XML для иерархии оборудования.

    Преобразует структуру путей в XML с учётом:
    - Пространств имён
    - Виртуальных контейнеров (через ParentObject)
    - ККС (теперь по новым правилам)
    - Связей ParentObject и ChildObjects
    """

    def __init__(self):
        self.config = get_config_manager().config
        xml_config = self.config["xml_generation"]
        self.logger = logging.getLogger("xml_generator")

        self.namespaces = xml_config["namespaces"]
        self.model_id = f"#_{xml_config['model_id']}"
        self.model_created = xml_config["model_created"]
        self.model_version = xml_config["model_version"]
        self.model_name = xml_config["model_name"]

        self.logger.info("XMLGenerator инициализирован")

    def _generate_id(self, path: Tuple[str, ...]) -> str:
        """
        Генерирует уникальный ID на основе UUID4.

        Args:
            path (Tuple[str, ...]): путь к объекту

        Returns:
            str: ID в формате "#_uuid"
        """
        uid = uuid4()
        self.logger.debug(f"Генерация UUID4 для пути {path}: {uid}")
        return f"#_{uid}"

    def generate(
        self,
        paths: List[Tuple[str, ...]],           # пути для создания
        # внешние дети (не используются)
        external_children: Dict[Tuple[str, ...], List[str]],
        parent_uid: str,                        # корневой родитель
        cck_map: Dict[Tuple[str, ...], str],    # ККС по путям
        parent_uid_map: Dict[Tuple[str, ...], str],  # виртуальные родители
        # виртуальные контейнеры (для логики)
        virtual_containers: Set[Tuple[str, ...]] = None
    ) -> str:
        """
        Основной метод генерации XML.

        Args:
            paths: список путей для создания объектов
            external_children: не используется (виртуальные дети отключены)
            parent_uid: UID корневого родителя
            cck_map: словарь {путь -> ККС}
            parent_uid_map: словарь {ребёнок -> UID виртуального родителя}
            virtual_containers: множество путей с UID (для справки)

        Returns:
            str: готовый XML как строка
        """
        self.logger.info("Начало генерации XML")
        if not paths:
            raise ValueError("Нет данных для генерации")

        cck_map = cck_map or {}
        parent_uid_map = parent_uid_map or {}
        virtual_containers = virtual_containers or set()

        # === Построение дерева ===
        all_nodes = set(paths)
        children_map = defaultdict(list)
        parent_map = {}

        # Строим иерархию: parent → [children]
        for path in paths:
            for i in range(1, len(path)):
                parent = tuple(path[:i])
                child = tuple(path[:i+1])
                if parent in paths and child in paths:
                    if child not in children_map[parent]:
                        children_map[parent].append(child)
                    parent_map[child] = parent

        # Генерируем ID для всех узлов
        id_map = {node: self._generate_id(node) for node in all_nodes}

        # === Генерация XML ===
        lines = []
        lines.append('<?xml version="1.0" encoding="utf-8"?>')
        lines.append('<?iec61970-552 version="2.0"?>')
        lines.append('<?floatExporter 1?>')

        # Открывающий тег RDF с пространствами имён
        rdf_open = '<rdf:RDF'
        for prefix, uri in self.namespaces.items():
            rdf_open += f' xmlns:{prefix}="{uri}"'
        rdf_open += '>'
        lines.append(rdf_open)

        # === FullModel ===
        lines.append(f'  <md:FullModel rdf:about="{self.model_id}">')
        lines.append(
            f'    <md:Model.created>{self.model_created}</md:Model.created>')
        lines.append(
            f'    <md:Model.version>{self.model_version}</md:Model.version>')
        lines.append(f'    <me:Model.name>{self.model_name}</me:Model.name>')
        lines.append('  </md:FullModel>')

        # === Генерация объектов ===
        processed = set()
        q = Queue()

        # Добавляем все пути в очередь
        for path in paths:
            q.put(path)

        while not q.empty():
            current = q.get()
            if current in processed:
                continue
            processed.add(current)

            current_id = id_map[current]
            is_leaf = (
                current not in children_map or not children_map[current])

            # Определяем тип объекта
            if len(current) == 1:
                element_type = "cim:AssetContainer"
            elif is_leaf:
                element_type = "me:GenericPSR"
            else:
                element_type = "cim:AssetContainer"

            # Начинаем тег
            lines.append(f'  <{element_type} rdf:about="{current_id}">')
            lines.append(
                f'    <cim:IdentifiedObject.name>{current[-1]}</cim:IdentifiedObject.name>')

            # === ParentObject (с приоритетом виртуальных родителей) ===
            if len(current) == 1:
                parent_resource = parent_uid
            else:
                if current in parent_uid_map:
                    parent_resource = f"#_{parent_uid_map[current]}"
                elif current in parent_map and parent_map[current] in id_map:
                    parent_resource = id_map[parent_map[current]]
                else:
                    parent_resource = parent_uid

            lines.append(
                f'    <me:IdentifiedObject.ParentObject rdf:resource="{parent_resource}" />')

            # === Связи Assets ===
            if element_type == "cim:AssetContainer":
                lines.append(
                    f'    <cim:Asset.AssetContainer rdf:resource="{parent_resource}" />')
            elif element_type == "me:GenericPSR":
                lines.append(
                    f'    <cim:PowerSystemResource.Assets rdf:resource="{parent_resource}" />')

            # === ЗАПИСЬ ККС ПО НОВЫМ ПРАВИЛАМ ===
            if current in cck_map and cck_map[current]:
                kks_code = cck_map[current]
                if element_type == "cim:AssetContainer":
                    # Для AssetContainer используем me:IdentifiedObject.mRIDStr
                    lines.append(
                        f'    <me:IdentifiedObject.mRIDStr>{kks_code}</me:IdentifiedObject.mRIDStr>')
                elif element_type == "me:GenericPSR":
                    # Для GenericPSR используем rh:PowerSystemResource.ccsCode
                    lines.append(
                        f'    <rh:PowerSystemResource.ccsCode>{kks_code}</rh:PowerSystemResource.ccsCode>')

            # === ChildObjects (только для AssetContainer) ===
            if element_type == "cim:AssetContainer":
                added_children = set()

                # Добавляем обычных детей
                if current in children_map:
                    for child in children_map[current]:
                        if child in id_map:
                            child_id = id_map[child]
                            if child_id not in added_children:
                                lines.append(
                                    f'    <me:IdentifiedObject.ChildObjects rdf:resource="{child_id}" />')
                                added_children.add(child_id)
                                q.put(child)
            # ВАЖНО: виртуальные контейнеры (с uid) НЕ добавляются как ChildObjects
            # Согласно требованиям, они существуют только как ParentObject для своих детей
            # if current in external_children:
            #     for uid in external_children[current]:
            #         ext_id = f"#_{uid}"
            #         if ext_id not in added_children:
            #             lines.append(
            #                 f'    <me:IdentifiedObject.ChildObjects rdf:resource="{ext_id}" />')
            #             added_children.add(ext_id)
                        # Закрываем тег
            lines.append(f'  </{element_type}>')

        lines.append('</rdf:RDF>')
        self.logger.info("Генерация XML завершена")
        return '\n'.join(lines)
