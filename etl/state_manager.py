import json
import os

class StateManager:
    """Класс для хранения состояния ETL."""
    def __init__(self, storage_file: str):
        self.storage_file = storage_file
        self.state = self._load_state()

    def _load_state(self) -> dict:
        """Загружает состояние из файла."""
        if os.path.exists(self.storage_file):
            with open(self.storage_file, 'r') as f:
                return json.load(f)
        return {}

    def get_state(self, key: str):
        """Получает состояние по ключу."""
        return self.state.get(key)

    def set_state(self, key: str, value):
        """Устанавливает состояние по ключу."""
        self.state[key] = value
        self._save_state()

    def _save_state(self):
        """Сохраняет состояние в файл."""
        with open(self.storage_file, 'w') as f:
            json.dump(self.state, f, indent=4)