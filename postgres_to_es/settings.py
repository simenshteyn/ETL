from pydantic import BaseModel


class DSNSettings(BaseModel):
    host: str
    port: int
    dbname: str
    password: str
    user: str


class PostgresSettings(BaseModel):
    dsn: DSNSettings
    chunk_size: int


class ElasticsearchSettings(BaseModel):
    protocol: str
    host: str
    port: int
    url: str
    alive_url: str
    headers: dict
    bulk_timeout: float


class StorageSettings(BaseModel):
    path: str


class EtlSettings(BaseModel):
    updates_check_period: int


class Config(BaseModel):
    movies_pg: PostgresSettings
    movies_es: ElasticsearchSettings
    storage: StorageSettings
    etl: EtlSettings
