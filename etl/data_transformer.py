class DataTransformer:
    """Класс для преобразования данных в формат Elasticsearch."""
    @staticmethod
    def transform_movie_data(data: dict) -> dict:
        """Преобразует данные фильма."""
        return {
            "id": data["id"],
            "imdb_rating": data["imdb_rating"],
            "genres": data["genres"],
            "title": data["title"],
            "description": data["description"],
            "directors_names": [d["name"] for d in data.get("directors", [])],
            "actors_names": [a["name"] for a in data.get("actors", [])],
            "writers_names": [w["name"] for w in data.get("writers", [])],
            "directors": data.get("directors", []),
            "actors": data.get("actors", []),
            "writers": data.get("writers", [])
        }