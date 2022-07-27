import psycopg2
import time
from contextlib import contextmanager
from datetime import datetime
from requests import post
from threading import Thread

from state import State, JsonFileStorage
from config import settings

CHUNK_SIZE = 100
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


@contextmanager
def postgres_conn_context():
    dbname = settings.DATABASES['default']['NAME']
    user = settings.DATABASES['default']['USER']
    password = settings.DATABASES['default']['PASSWORD']
    host = settings.DATABASES['default']['HOST']
    port = settings.DATABASES['default']['PORT']
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    yield conn
    conn.close()


def extract(state: State):
    with postgres_conn_context() as db_conn:
        initial_timestamp = state.get_state('initial_timestamp')
        if not initial_timestamp:
            initial_timestamp = datetime.now().strftime(DATETIME_FORMAT)
            state.set_state('initial_timestamp', initial_timestamp)
        while True:
            extracted_data = state.get_state('extracted_data')
            if not extracted_data:
                extracted_data = list()
                modified = state.get_state('modified', '2000-01-01 00:00:00.000000')
                cursor = db_conn.cursor()
                cursor.execute(f"""
                    SELECT fw.modified, fw.id, fw.rating, fw.title, fw.description,
                        array_agg(DISTINCT genre.name) genres,
                        array_agg(DISTINCT director.full_name) FILTER (WHERE director.full_name is not null) directors,
                        array_agg(DISTINCT actor.full_name) FILTER (WHERE actor.full_name is not null) actors_names,
                        array_agg(DISTINCT writer.full_name) FILTER (WHERE writer.full_name is not null) writers_names,
                        COALESCE (
                            json_agg(
                               DISTINCT jsonb_build_object(
                                   'id', actor.id,
                                   'name', actor.full_name
                               )
                            ) FILTER (WHERE actor.id is not null),
                            '[]'
                        ) as actors,
                        COALESCE (
                            json_agg(
                               DISTINCT jsonb_build_object(
                                   'id', writer.id,
                                   'name', writer.full_name
                               )
                            ) FILTER (WHERE writer.id is not null),
                            '[]'
                        ) as writers
                    FROM content.film_work fw
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre genre ON genre.id = gfw.genre_id
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person director ON director.id = pfw.person_id AND pfw.role = 'director'
                    LEFT JOIN content.person actor ON actor.id = pfw.person_id AND pfw.role = 'actor'
                    LEFT JOIN content.person writer ON writer.id = pfw.person_id AND pfw.role = 'writer'
                    WHERE fw.modified > '{modified}'
                    GROUP BY fw.id
                    ORDER BY fw.modified
                    LIMIT {CHUNK_SIZE}; 
                """)

                db_rows = cursor.fetchall()
                if db_rows:
                    last_modified = db_rows[-1][0].strftime(DATETIME_FORMAT)
                    state.set_state('modified', last_modified)
                    for row in db_rows:
                        extracted_data.append(row[1:])
                    state.set_state('extracted_data', extracted_data)
            time.sleep(1)


def transform(state: State):
    while True:
        transformed_data = state.get_state('transformed_data')
        if not transformed_data:
            transformed_data = dict()
            extracted_data = state.get_state('extracted_data')
            if extracted_data:
                for row in extracted_data:
                    transformed_data[row[0]] = {
                        "id": row[0],
                        "imdb_rating": row[1],
                        "title": row[2],
                        "description": row[3],
                        "genre": row[4] if row[4] else [],
                        "director": row[5] if row[5] else [],
                        "actors_names": row[6] if row[6] else [],
                        "writers_names": row[7] if row[7] else [],
                        "actors": row[8] if row[8] else [],
                        "writers": row[9] if row[9] else [],
                    }
                state.set_state('transformed_data', transformed_data)
                state.set_state('extracted_data', None)
        time.sleep(1)


def load(state: State):
    while True:
        transformed_data = state.get_state('transformed_data')
        if transformed_data:
            for k, v in transformed_data.items():
                response = post(f'http://127.0.0.1:9200/movies/_doc/{k}', json=v)
                print(response.json())
            state.set_state('transformed_data', None)
        else:
            time.sleep(1)


storage = JsonFileStorage(file_path='storage.json')
state_ = State(storage=storage)
extractor = Thread(target=extract, args=(state_,))
transformer = Thread(target=transform, args=(state_,))
loader = Thread(target=load, args=(state_,))
extractor.start()
transformer.start()
loader.start()
extractor.join()
transformer.join()
loader.join()


if __name__ == '__main__':
    pass
