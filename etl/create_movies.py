import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os

load_dotenv()

# Параметры подключения к PostgreSQL
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# SQL-запрос для создания таблицы
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS movies (
    id UUID PRIMARY KEY,
    imdb_rating REAL,
    genres JSONB NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    directors JSONB NOT NULL,
    actors JSONB NOT NULL,
    writers JSONB NOT NULL
);
"""


def create_database_and_table():
    """Создаёт базу данных и таблицу movies."""
    try:
        # Подключаемся к PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # Для создания базы данных
        cursor = conn.cursor()

        # Создаём базу данных, если её нет
        cursor.execute(f"CREATE DATABASE {DB_NAME};")
        print(f"База данных {DB_NAME} успешно создана.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"База данных {DB_NAME} уже существует.")
    finally:
        cursor.close()
        conn.close()

    # Подключаемся к созданной базе данных и создаём таблицу
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()

        # Выполняем SQL-запрос для создания таблицы
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        print("Таблица movies успешно создана.")
    except Exception as e:
        print(f"Ошибка при создании таблицы: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_database_and_table()
