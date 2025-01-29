from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging

class ElasticsearchLoader:
    """Класс для загрузки данных в Elasticsearch."""
    def __init__(self, host: str):
        self.es = Elasticsearch(hosts=[host])
        self.logger = logging.getLogger(__name__)

    def load_data(self, index: str, data: list):
        """Загружает данные в Elasticsearch."""
        actions = [
            {
                "_index": index,
                "_id": item["id"],
                "_source": item
            }
            for item in data
        ]
        bulk(self.es, actions)
        self.logger.info(f"Загружено {len(data)} документов в индекс {index}.")