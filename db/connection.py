import os
import sqlite3

import psycopg
from dotenv import load_dotenv

load_dotenv()


def get_sqlite_connection(database_path: str):
    return sqlite3.connect(database_path)


def get_postgres_connection(database_url: str):
    return psycopg.connect(database_url)