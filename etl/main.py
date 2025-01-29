from etl.data_transformer import DataTransformer
from etl.elasticsearch_loader import ElasticsearchLoader
from etl.etl_service import ETLService
from etl.postgres_extractor import PostgresExtractor
from etl.state_manager import StateManager

if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    # Настройки
    POSTGRES_DSN = {
        "dbname": "movies_db",
        "user": "postgres",
        "password": "password",
        "host": "db",
        "port": 5432
    }
    ELASTIC_HOST = "http://elasticsearch:9200"
    STATE_FILE = "state.json"
    MOVIES_QUERY = """
        SELECT id, imdb_rating, genres, title, description, directors, actors, writers
        FROM movies
        WHERE id > %s
        ORDER BY id ASC;
    """
    INDEX_NAME = "movies"

    # Инициализация компонентов
    state_manager = StateManager(STATE_FILE)
    extractor = PostgresExtractor(POSTGRES_DSN)
    transformer = DataTransformer()
    loader = ElasticsearchLoader(ELASTIC_HOST)
    etl_service = ETLService(extractor, transformer, loader, state_manager, MOVIES_QUERY, INDEX_NAME)

    # Запуск ETL
    etl_service.run()