import logging
import time
from backoff import on_exception, expo

class ETLService:
    """Класс ETL-сервиса."""
    def __init__(self, extractor, transformer, loader, state_manager, query, index):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.state_manager = state_manager
        self.query = query
        self.index = index
        self.logger = logging.getLogger(__name__)

    @on_exception(expo, Exception, max_tries=5)
    def run(self):
        """Основной процесс ETL."""
        last_id = self.state_manager.get_state("last_id") or 0
        self.logger.info(f"Начало обработки с ID: {last_id}")

        while True:
            data = list(self.extractor.extract(self.query, (last_id,)))
            if not data:
                self.logger.info("Новых данных не найдено. Ожидание...")
                time.sleep(10)  # Задержка перед следующей проверкой
                continue

            transformed_data = [self.transformer.transform_movie_data(row) for row in data]
            self.loader.load_data(self.index, transformed_data)

            last_id = max(item["id"] for item in data)
            self.state_manager.set_state("last_id", last_id)