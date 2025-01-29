import logging
import os
from dotenv import load_dotenv

from state_manager import StateManager
from postgres_extractor import PostgresExtractor
from data_transformer import DataTransformer
from elasticsearch_loader import ElasticsearchLoader
from etl_service import ETLService

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Загрузка переменных из .env
    load_dotenv()

    POSTGRES_DSN = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT")
    }
    ELASTIC_HOST = os.getenv("ELASTIC_HOST")
    STATE_FILE = os.getenv("STATE_FILE")
    INDEX_NAME = os.getenv("MOVIES_INDEX")

    MOVIES_QUERY = """
        SELECT id, imdb_rating, genres, title, description, directors, actors, writers
        FROM movies
        WHERE id > %s
        ORDER BY id ASC;
    """

    # Инициализация компонентов
    state_manager = StateManager(STATE_FILE)
    extractor = PostgresExtractor(POSTGRES_DSN)
    transformer = DataTransformer()
    loader = ElasticsearchLoader(ELASTIC_HOST)
    etl_service = ETLService(extractor, transformer, loader, state_manager, MOVIES_QUERY, INDEX_NAME)

    # Запуск ETL
    etl_service.run()