import logging
import os
from datetime import datetime
import time

import psycopg2
import requests
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import abc
import json
import logging
from typing import Optional, Any

from functools import wraps


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


def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=logger):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """

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

        return f_retry  # true decorator

    return deco_retry


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        if self.file_path is None:
            return

        with open(self.file_path, 'w') as f:
            json.dump(state, f)

    def retrieve_state(self) -> dict:
        if self.file_path is None:
            logging.info(
                'No state file provided. Continue with in-memory state')
            return {}

        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)

            return data

        except FileNotFoundError:
            self.save_state({})


class State:
    """
     Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.retrieve_state()

    def retrieve_state(self) -> dict:
        data = self.storage.retrieve_state()
        if not data:
            return {}
        return data

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.state[key] = value

        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.state.get(key)


class PgExtractor:
    """Connects and extracts data from Postgres in generator form."""

    def __init__(self, state: State, dsl: dict):
        self.state = state
        self.connection = None
        self.dsl = dsl

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

    def extract_movies(self, chunk_size=100):
        if self.is_connected():
            curs = self.connection.cursor()
            updated_time = self.state.get_state('movies_updated_at')
            if not updated_time:
                self.state.set_state('movies_updated_at', '1970-01-01')
                updated_time = self.state.get_state('movies_updated_at')
            try:
                curs.execute(f"""SELECT movie_id,
                                        movie_title,
                                        movie_desc,
                                        movie_rating,
                                        updated_at
                                   FROM content.movies
                                  WHERE updated_at > '{updated_time}'
                               ORDER BY updated_at;
                             """)
                while title_list := curs.fetchmany(chunk_size):
                    self.state.set_state('movies_updated_at',
                                         str(title_list[-1][-1]))
                    yield title_list
            except Exception as e:
                print(f'Error {e}')
            finally:
                curs.close()
        # TODO: добавить подключение в случае отсутствия связи, с реквизитами,
        # загружаемыми из dsl в хранилище
        else:
            print('trying to connect')
            self.connect()
            self.extract_movies(chunk_size)


class DataTransformer:
    """Tranforms data from PgExtractor into JSON optimized dictionary."""

    def __init__(self, extractor: PgExtractor):
        self.extractor = extractor

    def transform_movies(self, chunk_size=100):
        for movie_list in (
        movies := self.extractor.extract_movies(chunk_size)):
            result = []
            for movie in movie_list:
                movie_dict = {k: v for k, v in zip(['_id',
                                                    'title',
                                                    'description',
                                                    'imdb_rating'], movie)}
                # result.append(movie_dict)
                result.append(
                    {"index": {"_index": "movies", "_id": movie_dict['_id']}})
                del movie_dict['_id']
                if movie_dict['imdb_rating']:
                    movie_dict['imdb_rating'] = float(
                        movie_dict['imdb_rating'])
                result.append(movie_dict)
            payload = '\n'.join([json.dumps(line) for line in result]) + '\n'
            yield payload


class EsUploader:
    """Loads JSON data from DataTransforer via requests into Elasticsearch."""

    def __init__(self, dsl: dict, timeout=1):
        self.dsl = dsl
        self.timeout = timeout
        self.headers = {'Content-Type': 'application/x-ndjson'}
        self.url = '/_bulk?filter_path=items.*.error'

    def upload_movies(self, source: DataTransformer):
        movies_source = source.transform_movies()
        url = 'http://' + self.dsl['eshost'] \
              + ':' + self.dsl['esport'] + self.url
        for movies in movies_source:
            response = requests.post(url, data=movies, headers=self.headers)
            print(response.content)
            time.sleep(self.timeout)


class EtlManager:
    """Management of ETL process."""
    def __init__(self, dsl_pg: dict, dsl_es: dict):
        self.dsl_pg = dsl_pg
        self.dsl_es = dsl_es

    def start(self):
        storage = JsonFileStorage('storage.json')
        state = State(storage)
        pge = PgExtractor(state, self.dsl_pg)
        pge.connect()
        dtf = DataTransformer(pge)
        uploader = EsUploader(self.dsl_es)

        while True:
            uploader.upload_movies(dtf)
            time.sleep(5)
            print('checking updates')


def main():
    print('ETL service started')
    app = EtlManager(dsl_pg, dsl_es)
    app.start()


if __name__ == '__main__':
    dsl_pg = {'dbname': os.environ.get('DB_NAME', 'movies'),
              'user': os.environ.get('DB_USER', 'postgres'),
              'password': os.environ.get('DB_PASSWORD', 'yandex01'),
              'host': os.environ.get('DB_HOST', '127.0.0.1'),
              'port': os.environ.get('DB_PORT', '5432'),
              }

    dsl_es = {'eshost': os.environ.get('ES_HOST', '127.0.0.1'),
              'esport': os.environ.get('ES_PORT', '9200'),
              'esurl': '/_bulk?filter_path=items.*.error'}

    main()