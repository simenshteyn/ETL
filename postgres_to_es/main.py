import signal
import json
import sys
import time
from http import HTTPStatus
import requests

import psycopg2
from psycopg2.extras import DictCursor

from state import State, JsonFileStorage
from settings import Config
from log import get_logger
from back_off import backoff

logger = get_logger()


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
        return bool(self.connection and self.connection.closed == 0)

    @backoff(Exception, logger=logger)
    def connect(self):
        try:
            pg_conn = psycopg2.connect(**self.dsl, cursor_factory=DictCursor)
            self.connection = pg_conn
        except Exception:
            self.connection = None
            raise

    def disconnect(self):
        try:
            self.connection.close()
        except Exception as e:
            logger.debug(f'Error {e}')
        finally:
            self.connection = None

    def get_updated_time(self) -> str:
        updated_time = self.state.get_state('movies_updated_at')
        if not updated_time:
            self.state.set_state('movies_updated_at', '1970-01-01')
            updated_time = self.state.get_state('movies_updated_at')
        return updated_time

    @backoff(Exception, logger=logger)
    def check_updated_movies(self) -> bool:
        logger.info('Checking movie updates...')
        curs = self.connection.cursor()
        updated_time = self.get_updated_time()
        curs.execute("""SELECT movie_id, updated_at
                          FROM content.movies
                         WHERE updated_at > %s
                         LIMIT 1;""", (updated_time,))
        if any_updates := curs.fetchone():
            logger.info('Some movies updated')
            curs.close()
            return True
        else:
            logger.info('No movies updated')
            curs.close()
            return False

    @backoff(Exception, logger=logger)
    def extract_updated_movies(self):
        curs = self.connection.cursor()
        updated_time = self.get_updated_time()
        curs.execute("""
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
 WHERE m.updated_at > %s
 GROUP BY m.movie_id, m.movie_title, m.movie_desc, m.movie_rating;
""", (updated_time,))
        while title_list := curs.fetchmany(self.chunk_size):
            yield title_list
        curs.close()


class DataTransformer:
    """Tranforms data from PgExtractor into JSON optimized data string."""

    def __init__(self, extractor: PgExtractor):
        self.extractor = extractor

    def transform_movies(self):
        for movie_list in (
                movies := self.extractor.extract_updated_movies()):
            result = []
            for movie in movie_list:
                movie_dict = dict(zip(['_id',
                                       'imdb_rating',
                                       'genre',
                                       'title',
                                       'description',
                                       'director',
                                       'actors_names',
                                       'writers_names',
                                       'actors',
                                       'writers',
                                       'updated_at'
                                       ], movie))
                result.append(
                    {"index": {"_index": "movies", "_id": movie_dict['_id']}})
                del movie_dict['_id']
                self.extractor.state.set_state('movies_updated_at',
                                               str(movie_dict['updated_at']))
                del movie_dict['updated_at']
                if movie_dict['imdb_rating']:
                    movie_dict['imdb_rating'] = float(
                        movie_dict['imdb_rating'])
                result.append(movie_dict)
            payload = '\n'.join([json.dumps(line) for line in result]) + '\n'
            logger.debug(f'{payload[:120]}...')
            yield payload


class EsUploader:
    """Loads JSON data from DataTransforer via requests into Elasticsearch."""

    def __init__(self, config: Config):
        self.config = config
        self.alive_url = '{protocol}://{host}:{port}{path}'.format(
            protocol=self.config.movies_es.protocol,
            host=self.config.movies_es.host,
            port=self.config.movies_es.port,
            path=self.config.movies_es.alive_url
        )

    @backoff(Exception, logger=logger)
    def server_check(self):
        response = requests.get(self.alive_url)
        if response.status_code != HTTPStatus.OK:
            raise ConnectionError('ES connection error')

    def is_alive(self) -> bool:
        try:
            response = requests.get(self.alive_url)
            if response.status_code == HTTPStatus.OK:
                return True
            else:
                logger.debug(f'ES server NOT alive: {response.status_code}')
                return False
        except Exception as e:
            logger.debug(f'Error {e}')
            return False

    @backoff(Exception, logger=logger)
    def upload_movies(self, source: DataTransformer):
        movies_source = source.transform_movies()
        url = '{protocol}://{host}:{port}{path}'.format(
            protocol=self.config.movies_es.protocol,
            host=self.config.movies_es.host,
            port=self.config.movies_es.port,
            path=self.config.movies_es.url
        )
        for movies in movies_source:
            try:
                response = requests.post(url, data=movies,
                                         headers=self.config.movies_es.headers)
                logger.info(f'{response.content}')
                time.sleep(self.config.movies_es.bulk_timeout)
            except Exception as e:
                raise Exception(f'Exception {e}')


class EtlManager:
    """Management of ETL process."""

    def __init__(self, config: Config):
        self.config = config
        storage = JsonFileStorage(self.config.storage.path)
        self.state = State(storage)
        self.extractor = PgExtractor(self.state, self.config)
        self.transformer = DataTransformer(self.extractor)
        self.uploader = EsUploader(self.config)
        self.graceful_exit = False

    def start(self):
        while True:
            self._execute()

    def stop(self):
        self.graceful_exit = True

    def _execute(self):
        self.extractor.connect()
        if not self.extractor.is_connected():
            return
        self.uploader.server_check()
        if not self.uploader.is_alive():
            return
        if self.extractor.check_updated_movies():
            self.uploader.upload_movies(self.transformer)
        self.extractor.disconnect()
        if self.graceful_exit:
            logger.info('Terminating app gracefully...')
            sys.exit()
        time.sleep(self.config.etl.updates_check_period)


def main():
    logger.info('ETL service started')
    config = Config.parse_file('config.json')
    app = EtlManager(config)
    signal.signal(signal.SIGTERM, app.stop)
    signal.signal(signal.SIGINT, app.stop)
    app.start()


if __name__ == '__main__':
    main()
