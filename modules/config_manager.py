"""
Модуль управления конфигурацией
"""

import json
from typing import Dict, Any
from pathlib import Path


class ConfigManager:
    """Менеджер конфигурации."""

    def __init__(self, config_path: str = "config.json"):
        """
        Инициализация менеджера конфигурации.

        Args:
            config_path: путь к файлу конфигурации
        """
        self.config_path = Path(config_path)
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Загружает конфигурацию из файла."""
        if not self.config_path.exists():
            # Создаем дефолтную конфигурацию
            default_config = self._get_default_config()
            self._save_config(default_config)
            return default_config

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Ошибка загрузки конфигурации: {e}")

    def _get_default_config(self) -> Dict[str, Any]:
        """Возвращает конфигурацию по умолчанию."""
        return {
            "xml_generation": {
                "namespaces": {
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "md": "http://iec.ch/TC57/61970-552/ModelDescription/1#",
                    "cim": "http://monitel.com/2021/schema-access#"
                },
                "me_namespace": "http://monitel.com/2014/schema-cim16#",
                "model_created": "2025-08-20T11:50:08.2085734Z",
                "model_version": "ver:11.7.1.254;ext:6;",
                "model_name": "CIM16"
            },
            "file_management": {
                "exclude_files": ["Sample.csv"],
                "log_directory": "log",
                "input_directory": "input",
                "output_directory": "output"
            },
            "logging": {
                "level": "DEBUG",
                "format": "%(asctime)s [%(levelname)s]: %(message)s",
                "date_format": "%Y-%m-%d %H:%M:%S"
            }
        }

    def _save_config(self, config: Dict[str, Any]) -> None:
        """Сохраняет конфигурацию в файл."""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise Exception(f"Ошибка сохранения конфигурации: {e}")

    def get(self, key_path: str, default=None):
        """
        Получает значение по пути к ключу.

        Args:
            key_path: путь к ключу через точку (например, "csv_processing.required_fields")
            default: значение по умолчанию

        Returns:
            Значение или default
        """
        keys = key_path.split('.')
        value = self._config

        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key_path: str, value) -> None:
        """
        Устанавливает значение по пути к ключу.

        Args:
            key_path: путь к ключу через точку
            value: значение для установки
        """
        keys = key_path.split('.')
        config = self._config

        # Создаем вложенные словари если их нет
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]

        config[keys[-1]] = value
        self._save_config(self._config)

    @property
    def config(self) -> Dict[str, Any]:
        """Возвращает полную конфигурацию."""
        return self._config.copy()


# Глобальный экземпляр конфигурации
_config_manager = None


def get_config_manager(config_path: str = "config.json") -> ConfigManager:
    """
    Получает менеджер конфигурации (singleton).

    Args:
        config_path: путь к файлу конфигурации

    Returns:
        ConfigManager: экземпляр менеджера конфигурации
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(config_path)
    return _config_manager


def get_config_value(key_path: str, default=None):
    """
    Получает значение конфигурации по ключу.

    Args:
        key_path: путь к ключу
        default: значение по умолчанию

    Returns:
        Значение конфигурации
    """
    return get_config_manager().get(key_path, default)
