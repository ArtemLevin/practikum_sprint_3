import psycopg2
from psycopg2.extras import DictCursor
from typing import Generator

class PostgresExtractor:
    """Класс для извлечения данных из PostgreSQL."""
    def __init__(self, dsn: dict):
        self.dsn = dsn

    def extract(self, query: str, params: tuple = ()) -> Generator[dict, None, None]:
        """Извлекает данные из PostgreSQL."""
        with psycopg2.connect(**self.dsn) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, params)
                while rows := cursor.fetchmany(500):  # Пакетная обработка
                    for row in rows:
                        yield dict(row)