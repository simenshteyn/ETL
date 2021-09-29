import os
import json
import time
import logging
import requests
from functools import wraps

import psycopg2
from psycopg2.extras import DictCursor

from postgres_to_es.state import State, JsonFileStorage
from settings import Config


def get_logger() -> logging.Logger:
    """Get and set logging for debug and measure performance."""
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')
    handler = logging.StreamHandler()
    log_format = '%(asctime)s %(levelname)s -- %(message)s'
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = get_logger()


def backoff(ExceptionToCheck, tries=16, delay=0.1, backoff=2, logger=logger):
    """Retry calling the decorated function using an exponential backoff."""

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry

    return deco_retry


class PgExtractor:
    """Connects and extracts data from Postgres in generator form."""

    def __init__(self, state: State, config: Config):
        self.state = state
        self.connection = None
        self.config = config
        self.dsl = {'dbname': config.movies_pg.dsn.dbname,
                    'user': config.movies_pg.dsn.user,
                    'password': config.movies_pg.dsn.password,
                    'host': config.movies_pg.dsn.host,
                    'port': config.movies_pg.dsn.port,
                    }
        self.chunk_size = config.movies_pg.chunk_size

    def is_connected(self) -> bool:
        if self.connection:
            return True
        else:
            return False

    def connect(self) -> bool:
        try:
            pg_conn = psycopg2.connect(**self.dsl, cursor_factory=DictCursor)
            self.connection = pg_conn
            return True
        except Exception as e:
            print(f'Error {e}')
            self.connection = None
            return False

    def extract_movies(self):
        if self.is_connected():
            curs = self.connection.cursor()
            updated_time = self.state.get_state('movies_updated_at')
            if not updated_time:
                self.state.set_state('movies_updated_at', '1970-01-01')
                updated_time = self.state.get_state('movies_updated_at')
            try:
                curs.execute(f"""
SELECT m.movie_id,
       m.movie_rating as imdb_rating,
       ARRAY_AGG(DISTINCT g.genre_name) AS genre,
       m.movie_title,
       m.movie_desc,
       ARRAY_AGG(DISTINCT p.full_name)
           FILTER (WHERE mp.person_role = 'director') AS director,
       ARRAY_AGG(DISTINCT p.full_name)
           FILTER (WHERE mp.person_role = 'actor') AS actors_names,  
       ARRAY_AGG(DISTINCT p.full_name)
           FILTER (WHERE mp.person_role = 'writer') AS writers_names,                                                        
       JSON_AGG(DISTINCT jsonb_build_object('id', p.person_id,
                                            'name', p.full_name))
           FILTER (WHERE mp.person_role = 'actor') AS actors,
       JSON_AGG(DISTINCT jsonb_build_object('id', p.person_id,
                                            'name', p.full_name))
           FILTER (WHERE mp.person_role = 'writer') AS writers,
       m.updated_at
  FROM content.movies AS m
  LEFT JOIN content.movie_genres AS mg
            ON m.movie_id = mg.movie_id
  LEFT JOIN content.genres AS g
            ON mg.genre_id = g.genre_id
  LEFT JOIN content.movie_people AS mp
            ON m.movie_id = mp.movie_id
  LEFT JOIN content.people AS p
            ON mp.person_id = p.person_id
 WHERE m.updated_at > '{updated_time}'
 GROUP BY m.movie_id, m.movie_title, m.movie_desc, m.movie_rating;
""")
                while title_list := curs.fetchmany(self.chunk_size):
                    self.state.set_state('movies_updated_at',
                                         str(title_list[-1][-1]))
                    yield title_list
            except Exception as e:
                print(f'Error {e}')
            finally:
                curs.close()


class DataTransformer:
    """Tranforms data from PgExtractor into JSON optimized dictionary."""

    def __init__(self, extractor: PgExtractor):
        self.extractor = extractor

    def transform_movies(self):
        for movie_list in (
                movies := self.extractor.extract_movies()):
            result = []
            for movie in movie_list:
                movie_dict = {k: v for k, v in zip(['_id',
                                                    'imdb_rating',
                                                    'genre',
                                                    'title',
                                                    'description',
                                                    'director',
                                                    'actors_names',
                                                    'writers_names',
                                                    'actors',
                                                    'writers'
                                                    ], movie)}
                result.append(
                    {"index": {"_index": "movies", "_id": movie_dict['_id']}})
                del movie_dict['_id']
                if movie_dict['imdb_rating']:
                    movie_dict['imdb_rating'] = float(
                        movie_dict['imdb_rating'])
                result.append(movie_dict)
            payload = '\n'.join([json.dumps(line) for line in result]) + '\n'
            print(payload)
            yield payload


class EsUploader:
    """Loads JSON data from DataTransforer via requests into Elasticsearch."""

    def __init__(self, config: Config):
        self.config = config

    def upload_movies(self, source: DataTransformer):
        movies_source = source.transform_movies()
        url = self.config.movies_es.protocol + '://' \
              + self.config.movies_es.host \
              + ':' + str(self.config.movies_es.port) \
              + self.config.movies_es.url
        for movies in movies_source:
            response = requests.post(url, data=movies,
                                     headers=self.config.movies_es.headers)
            print(response.content)
            time.sleep(self.config.movies_es.bulk_timeout)


class EtlManager:
    """Management of ETL process."""

    def __init__(self, config: Config):
        self.config = config

    def start(self):
        storage = JsonFileStorage(self.config.storage.path)
        state = State(storage)
        extractor = PgExtractor(state, self.config)
        extractor.connect()
        transformer = DataTransformer(extractor)
        uploader = EsUploader(self.config)

        while True:
            uploader.upload_movies(transformer)
            time.sleep(self.config.etl.updates_check_period)
            print('checking updates')


def main():
    print('ETL service started')
    config = Config.parse_file('config.json')
    app = EtlManager(config)
    app.start()


if __name__ == '__main__':
    main()
