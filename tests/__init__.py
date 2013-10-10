from __future__ import absolute_import

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def setup_package():
    _run_postgres_cmd('DROP DATABASE IF EXISTS pymongres_test')
    _run_postgres_cmd('CREATE DATABASE pymongres_test')


def teardown_package():
    _run_postgres_cmd('DROP DATABASE pymongres_test')


def _run_postgres_cmd(command):
    with connect(database='postgres') as connection:
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with connection.cursor() as cursor:
            cursor.execute(command)
