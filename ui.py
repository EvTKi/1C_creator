import sys
import os
import threading
from pathlib import Path
from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt, QMetaObject, Q_ARG, QProcess, QT_VERSION_STR, QPropertyAnimation
from PyQt5.QtGui import QPalette, QColor, QIcon, QFont
from PyQt5.QtWidgets import QGraphicsOpacityEffect

import sys
import os
import threading
from pathlib import Path
from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt, QMetaObject, Q_ARG, QProcess
from PyQt5.QtGui import QPalette, QColor, QIcon, QFont


def resource_path(relative_path):
    """Получение правильного пути к ресурсу, работает как для скрипта, так и для .exe"""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


def get_config_value(key, default=None):
    return default


class CSVProcessorApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.file_checkboxes = []
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Конвертер CSV → RDF/XML')
        self.setGeometry(100, 100, 950, 750)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # Настройки
        settings_group = QGroupBox("⚙️ Настройки обработки")
        settings_layout = QGridLayout()
        settings_layout.setSpacing(10)
        settings_layout.setContentsMargins(15, 15, 15, 15)

        # UID
        uid_label = QLabel("UID папки для ролей:")
        self.uid_input = QLineEdit()
        self.uid_input.setPlaceholderText("Введите UID папки...")
        settings_layout.addWidget(uid_label, 0, 0)
        settings_layout.addWidget(self.uid_input, 0, 1, 1, 2)

        # Папка CSV
        csv_label = QLabel("📁 Папка с CSV-файлами:")
        self.csv_path_input = QLineEdit()
        self.csv_path_input.setPlaceholderText(
            "Выберите папку с CSV файлами...")
        self.browse_button = QPushButton("📂 Обзор")
        self.browse_button.setObjectName("browse_button")
        self.browse_button.clicked.connect(self.select_folder)
        settings_layout.addWidget(csv_label, 1, 0)
        settings_layout.addWidget(self.csv_path_input, 1, 1)
        settings_layout.addWidget(self.browse_button, 1, 2)

        settings_group.setLayout(settings_layout)
        main_layout.addWidget(settings_group)

        # Список файлов
        files_group = QGroupBox("📋 Выберите файлы для обработки")
        files_layout = QVBoxLayout()
        files_layout.setContentsMargins(15, 15, 15, 15)

        self.files_scroll = QScrollArea()
        self.files_scroll.setWidgetResizable(True)
        self.files_widget = QWidget()
        self.files_layout = QVBoxLayout(self.files_widget)
        self.files_layout.setSpacing(5)
        self.files_layout.setContentsMargins(10, 10, 10, 10)
        self.files_scroll.setWidget(self.files_widget)
        files_layout.addWidget(self.files_scroll)

        files_group.setLayout(files_layout)
        main_layout.addWidget(files_group)

        # Кнопки
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(15)

        self.start_button = QPushButton("🚀 Старт обработки")
        self.start_button.setObjectName("start_button")
        self.start_button.clicked.connect(self.start_conversion)

        # Восстанавливаем кнопку "Открыть папку"
        self.open_folder_button = QPushButton("📁 Открыть папку с результатами")
        self.open_folder_button.setObjectName("open_folder_button")
        self.open_folder_button.clicked.connect(self.open_results_folder)

        buttons_layout.addWidget(self.start_button)
        buttons_layout.addStretch()
        buttons_layout.addWidget(self.open_folder_button)

        main_layout.addLayout(buttons_layout)

        # Лог
        log_group = QGroupBox("📝 Протокол / лог")
        log_layout = QVBoxLayout()
        log_layout.setContentsMargins(15, 15, 15, 15)

        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(250)
        log_layout.addWidget(self.log_text)
        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)

        # Применяем стиль
        self.apply_styles()

    def apply_styles(self):
        """Применение стилей к интерфейсу"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f8f9fa;
                font-family: "Segoe UI", Arial, sans-serif;
                font-size: 10pt;
            }
            
            QGroupBox {
                font-weight: bold;
                border: 2px solid #e9ecef;
                border-radius: 10px;
                margin-top: 15px;
                padding-top: 20px;
                background-color: white;
                border: 1px solid #dee2e6;
            }
            
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 15px;
                padding: 0 8px 0 8px;
                color: #495057;
                background-color: white;
            }
            
            QLabel {
                color: #495057;
                font-weight: 500;
            }
            
            QLineEdit {
                padding: 12px;
                border: 2px solid #ced4da;
                border-radius: 8px;
                background-color: white;
                font-size: 11pt;
            }
            
            QLineEdit:focus {
                border-color: #007bff;
                outline: none;
            }
            
            QPushButton {
                padding: 12px 20px;
                border-radius: 8px;
                font-weight: bold;
                font-size: 11pt;
                min-height: 45px;
                text-align: center;
            }
            
            QPushButton#start_button {
                background-color: #28a745;
                color: white;
                border: none;
            }
            
            QPushButton#start_button:hover {
                background-color: #218838;
            }
            
            QPushButton#start_button:pressed {
                background-color: #1e7e34;
            }
            
            QPushButton#browse_button {
                background-color: #17a2b8;
                color: white;
                border: none;
            }
            
            QPushButton#browse_button:hover {
                background-color: #138496;
            }
            
            QPushButton#browse_button:pressed {
                background-color: #117a8b;
            }
            
            QPushButton#open_folder_button {
                background-color: #e74c3c;
                color: white;
                border: none;
                min-width: 250px;
            }
            
            QPushButton#open_folder_button:hover {
                background-color: #c0392b;
            }
            
            QPushButton#open_folder_button:pressed {
                background-color: #a93226;
            }
            
            QCheckBox {
                padding: 10px 12px;
                border-bottom: 1px solid #f0f0f0;
                spacing: 12px;
                border-radius: 6px;
                margin: 2px;
            }
            
            QCheckBox:hover {
                background-color: #f8f9fa;
            }
            
            QCheckBox:last-child {
                border-bottom: none;
            }
            
            QCheckBox::indicator {
                width: 20px;
                height: 20px;
                border: 2px solid #adb5bd;
                border-radius: 6px;
            }
            
            QCheckBox::indicator:unchecked {
                background-color: white;
            }
            
            QCheckBox::indicator:checked {
                background-color: #007bff;
                border-color: #007bff;
            }
            
            QScrollArea {
                border: 2px solid #e9ecef;
                border-radius: 8px;
                background-color: white;
            }
            
            QScrollBar:vertical {
                border: none;
                background: #f8f9fa;
                width: 16px;
                border-radius: 8px;
            }
            
            QScrollBar::handle:vertical {
                background: #adb5bd;
                border-radius: 8px;
                min-height: 30px;
            }
            
            QScrollBar::handle:vertical:hover {
                background: #868e96;
            }
            
            QScrollBar::add-line, QScrollBar::sub-line {
                border: none;
                background: none;
            }
            
            QPlainTextEdit {
                background-color: #ffffff;
                color: #212529;
                font-family: Consolas, Monaco, monospace;
                font-size: 11px;
                border: 2px solid #e9ecef;
                border-radius: 8px;
                padding: 12px;
            }
        """)

    def select_folder(self):
        folder = QFileDialog.getExistingDirectory(
            self, "Выберите папку с CSV файлами")
        if folder:
            self.csv_path_input.setText(folder)
            self.populate_file_list()

    def populate_file_list(self):
        for checkbox in self.file_checkboxes:
            checkbox.setParent(None)
        self.file_checkboxes.clear()

        folder = self.csv_path_input.text()
        if not folder or not os.path.isdir(folder):
            return

        try:
            files = [f for f in os.listdir(folder)
                     if f.lower().endswith('.csv') and f.lower() != 'sample.csv']
        except Exception as e:
            self.add_log(f"❌ Ошибка чтения папки: {e}\n")
            return

        if not files:
            no_files_label = QLabel(
                "📁 Нет подходящих CSV-файлов в выбранной папке")
            no_files_label.setStyleSheet(
                "color: #e74c3c; font-style: italic; padding: 15px; font-weight: normal;")
            self.files_layout.addWidget(no_files_label)
            self.file_checkboxes.append(no_files_label)
            return

        files.sort()

        for filename in files:
            checkbox = QCheckBox(filename)
            checkbox.setChecked(True)
            self.files_layout.addWidget(checkbox)
            self.file_checkboxes.append(checkbox)

        self.add_log(f"✅ Найдено {len(files)} CSV-файлов\n")

    def start_conversion(self):
        uid = self.uid_input.text().strip()
        csv_dir = self.csv_path_input.text()

        selected_files = []
        for checkbox in self.file_checkboxes:
            if isinstance(checkbox, QCheckBox) and checkbox.isChecked():
                selected_files.append(checkbox.text())

        if not uid:
            QMessageBox.warning(self, "⚠️ Внимание!",
                                "Введите UID папки для ролей!")
            return
        if not csv_dir or not os.path.isdir(csv_dir):
            QMessageBox.warning(self, "⚠️ Внимание!",
                                "Выберите действительную папку с CSV-файлами!")
            return
        if not selected_files:
            QMessageBox.warning(self, "⚠️ Внимание!",
                                "Выделите хотя бы 1 файл!")
            return

        self.log_text.clear()
        self.add_log("╔══════════════════════════════════════════╗\n")
        self.add_log("║           ЗАПУСК КОНВЕРТЕР СИСТЕМЫ        ║\n")
        self.add_log("╚══════════════════════════════════════════╝\n")
        self.add_log("\n")

        self.add_log("🖥️  СИСТЕМНАЯ ИНФОРМАЦИЯ\n")
        self.add_log(f"  • ОС: {sys.platform}\n")
        self.add_log(f"  • Python: {sys.version.split()[0]}\n")
        self.add_log(f"  • PyQt5: {QT_VERSION_STR}\n")
        self.add_log(
            f"  • Режим: {'exe' if getattr(sys, 'frozen', False) else 'script'}\n")

        self.add_log("\n📁  ПУТИ И ДИРЕКТОРИИ\n")
        self.add_log(f"  • Текущая директория: {os.getcwd()}\n")
        self.add_log(
            f"  • Директория скрипта: {os.path.dirname(os.path.abspath(__file__))}\n")

        self.add_log("\n⚙️  ПАРАМЕТРЫ ОБРАБОТКИ\n")
        self.add_log(f"  • UID папки: {uid}\n")
        self.add_log(f"  • CSV директория: {csv_dir}\n")
        self.add_log(
            f"  • Количество выбранных файлов: {len(selected_files)}\n")

        self.add_log("\n📋  ВЫБРАННЫЕ ФАЙЛЫ\n")
        for i, filename in enumerate(selected_files, 1):
            file_path = os.path.join(csv_dir, filename)
            try:
                file_size = os.path.getsize(file_path)
                size_kb = file_size / 1024
                self.add_log(f"  {i}. 📄 {filename} ({size_kb:.1f} KB)\n")
            except Exception as e:
                self.add_log(f"  {i}. ❌ {filename} (ошибка: {e})\n")

        self.add_log("\n" + "━" * 50 + "\n")
        self.add_log("🚀 НАЧАЛО ОБРАБОТКИ ФАЙЛОВ\n")
        self.add_log("━" * 50 + "\n")

        def run_job():
            try:
                if getattr(sys, 'frozen', False):
                    script_dir = os.path.dirname(sys.executable)
                    modules_path = os.path.join(script_dir, 'modules')
                    if modules_path not in sys.path:
                        sys.path.insert(0, modules_path)
                        self.add_log(
                            f"  🔧 Добавлен путь к modules: {modules_path}\n")

                try:
                    from main import process_file
                    self.add_log("  ✅ Импорт process_file успешен\n")
                except ImportError as e:
                    self.add_log(f"  ❌ Ошибка импорта main.py: {e}\n")
                    return

                success_count = 0
                error_count = 0

                for i, filename in enumerate(selected_files, 1):
                    csv_path = Path(csv_dir) / filename
                    self.add_log(
                        f"\n📄 [{i}/{len(selected_files)}] ОБРАБОТКА: {filename}\n")

                    try:
                        if not csv_path.exists():
                            self.add_log(f"    ❌ Файл не найден\n")
                            error_count += 1
                            continue

                        file_size = csv_path.stat().st_size
                        if file_size == 0:
                            self.add_log(f"    ⚠️  Файл пустой, пропускаем\n")
                            continue

                        self.add_log(f"    • Размер: {file_size} байт\n")
                        self.add_log(f"    🔄 Обработка...\n")

                        process_file(csv_path, uid)
                        self.add_log(f"    ✅ Успешно обработан\n")
                        success_count += 1

                    except Exception as e:
                        self.add_log(f"    ❌ Ошибка: {str(e)}\n")
                        error_count += 1

                self.add_log("\n" + "━" * 50 + "\n")
                self.add_log(
                    f"📊 РЕЗУЛЬТАТЫ: {success_count} успешно, {error_count} с ошибками\n")
                self.add_log("✅ ОБРАБОТКА ЗАВЕРШЕНА\n")
                self.add_log("━" * 50 + "\n")

            except Exception as e:
                import traceback
                self.add_log(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}\n")
                self.add_log(
                    f"📝 Traceback: {traceback.format_exc().splitlines()[-1]}\n")

        thread = threading.Thread(target=run_job, daemon=True)
        thread.start()

    def add_log(self, text):
        """Потокобезопасное добавление лога"""
        if hasattr(self, 'log_text'):
            from datetime import datetime
            timestamp = datetime.now().strftime("%H:%M:%S")
            formatted_text = f"[{timestamp}] {text}"

            QMetaObject.invokeMethod(
                self.log_text,
                "appendPlainText",
                Qt.QueuedConnection,
                Q_ARG(str, formatted_text)
            )
            QMetaObject.invokeMethod(
                self.log_text.verticalScrollBar(),
                "setValue",
                Qt.QueuedConnection,
                Q_ARG(int, self.log_text.verticalScrollBar().maximum())
            )

    def open_results_folder(self):
        folder = self.csv_path_input.text()
        if not folder or not os.path.isdir(folder):
            QMessageBox.information(
                self, "ℹ️ Инфо", "Папка не выбрана или не найдена.")
            return

        try:
            if sys.platform.startswith('win'):
                os.startfile(folder)
            elif sys.platform.startswith('darwin'):
                QProcess.startDetached('open', [folder])
            else:
                QProcess.startDetached('xdg-open', [folder])
            self.add_log(f"✅ Открыта папка: {folder}\n")
        except Exception as e:
            QMessageBox.critical(
                self, "❌ Ошибка", f"Не удалось открыть папку: {e}")


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    # Настройка палитры
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(248, 249, 250))
    palette.setColor(QPalette.WindowText, QColor(33, 37, 41))
    palette.setColor(QPalette.Base, QColor(255, 255, 255))
    palette.setColor(QPalette.AlternateBase, QColor(248, 249, 250))
    palette.setColor(QPalette.ToolTipBase, QColor(33, 37, 41))
    palette.setColor(QPalette.ToolTipText, QColor(33, 37, 41))
    palette.setColor(QPalette.Text, QColor(33, 37, 41))
    palette.setColor(QPalette.Button, QColor(248, 249, 250))
    palette.setColor(QPalette.ButtonText, QColor(33, 37, 41))
    palette.setColor(QPalette.BrightText, QColor(255, 255, 255))
    palette.setColor(QPalette.Link, QColor(0, 123, 255))
    palette.setColor(QPalette.Highlight, QColor(0, 123, 255))
    palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
    app.setPalette(palette)

    window = CSVProcessorApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
