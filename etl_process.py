import psycopg2
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from requests.exceptions import ConnectionError
from requests import post
from threading import Thread

import app_logging
from state import State, JsonFileStorage
from config import settings
from backoff import backoff

logger = app_logging.get_logger()
CHUNK_SIZE = 100
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

MAIN_SQL_SELECT = """
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
    LEFT JOIN content.person actor ON actor.id = pfw.person_id AND pfw.role = 'actor'
    LEFT JOIN content.person director ON director.id = pfw.person_id AND pfw.role = 'director'
    LEFT JOIN content.person writer ON writer.id = pfw.person_id AND pfw.role = 'writer'
"""


@backoff()
def postgres_conn():
    """
    Creates connection to postgresgl db with credentials from settings
    :return: connection
    """
    dbname = settings.DATABASES['default']['NAME']
    user = settings.DATABASES['default']['USER']
    password = settings.DATABASES['default']['PASSWORD']
    host = settings.DATABASES['default']['HOST']
    port = settings.DATABASES['default']['PORT']
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    return conn


@contextmanager
def postgres_conn_context():
    """
    Creates context for postgresql connection
    :return: connection
    """
    conn = postgres_conn()
    yield conn
    conn.close()


def extract(state: State) -> None:
    """
    Select filmworks data and all related data from db and save it in the state.
    Wait until data in state will be processed by transformer.
    :param state: state of storage
    """
    while True:
        with postgres_conn_context() as db_conn:
            extracted_data = state.get('extracted_data')
            if not extracted_data:
                extracted_data = list()

                timestamp = state.get('timestamp')
                if not timestamp:
                    timestamp = datetime.now(timezone.utc).strftime(DATETIME_FORMAT)
                    state.set('timestamp', timestamp)
                current_timestamp = datetime.now(timezone.utc).strftime(DATETIME_FORMAT)

                try:
                    cursor = db_conn.cursor()
                    # first we check if there are some changes on genres or persons since last iteration
                    # and get all related film works
                    cursor.execute(f"""
                        {MAIN_SQL_SELECT}
                        WHERE fw.id IN (
                            SELECT fw.id 
                            FROM content.film_work fw
                            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                            LEFT JOIN content.genre genre ON genre.id = gfw.genre_id
                            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                            LEFT JOIN content.person person ON person.id = pfw.person_id
                            WHERE genre.modified > '{timestamp}' OR person.modified > '{timestamp}'
                        )
                        GROUP BY fw.id; 
                    """)
                    db_rows = cursor.fetchall()
                    if not db_rows:
                        # in case there are no changes on genres or persons since last iteration
                        # we get chunk of film works by last processed 'modified'
                        fw_modified = state.get('fw_modified', '2000-01-01 00:00:00.000000')
                        cursor.execute(f"""
                            {MAIN_SQL_SELECT}
                            WHERE fw.modified > '{fw_modified}'
                            GROUP BY fw.id ORDER BY fw.modified
                            LIMIT {CHUNK_SIZE}; 
                        """)
                        db_rows = cursor.fetchall()
                        if db_rows:
                            set_state = 'fw_modified', db_rows[-1][0].strftime(DATETIME_FORMAT)
                    else:
                        set_state = 'timestamp', current_timestamp
                except psycopg2.Error as e:
                    logger.error(e)
                    continue

                if db_rows:
                    for row in db_rows:
                        extracted_data.append(row[1:])
                    state.set('extracted_data', extracted_data)
                    state.set(*set_state)
                    logger.info(f'extracted {len(db_rows)} rows')

            time.sleep(.1)


def transform(state: State) -> None:
    """
    Transform data prepared by extractor according to es schema and save it in the state.
    Wait until data in state will be processed by loader.
    :param state: state of storage
    """
    while True:
        transformed_data = state.get('transformed_data')
        if not transformed_data:
            transformed_data = dict()
            extracted_data = state.get('extracted_data')
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
                state.set('transformed_data', transformed_data)
                state.set('extracted_data', None)
                logger.info(f'transformed {len(transformed_data)} rows')
        time.sleep(.1)


def load(state: State) -> None:
    """
    Send data prepared by transformer to elasticsearch
    :param state: state of storage
    """
    while True:
        try:
            transformed_data = state.get('transformed_data')
            if transformed_data:
                for k, v in transformed_data.items():
                    post(f'{settings.ES_URL}/{settings.ES_INDEX_NAME}/_doc/{k}', json=v)
                state.set('transformed_data', None)
                logger.info(f'loaded {len(transformed_data)} rows')
            else:
                time.sleep(.1)
        except ConnectionError as e:
            logger.error(e)
            time.sleep(1)
            continue


storage = JsonFileStorage(file_path='storage.json')
storage_state = State(storage=storage)
extractor = Thread(target=extract, args=(storage_state,))
transformer = Thread(target=transform, args=(storage_state,))
loader = Thread(target=load, args=(storage_state,))
extractor.start()
transformer.start()
loader.start()
extractor.join()
transformer.join()
loader.join()


if __name__ == '__main__':
    pass
