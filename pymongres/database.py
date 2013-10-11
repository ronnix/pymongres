from __future__ import absolute_import

import psycopg2

from pymongres.collection import Collection


class Database(object):

    def __init__(self, client, name):
        self.client = client
        self.name = name

    def connection(self):
        return psycopg2.connect(database=self.name, **self.client.kwargs)

    def collection_names(self):
        return [name for name in self._list_tables() if not name.startswith(('pg_', 'sql_'))]

    def drop_collection(self, name):
        self._drop_table(name)

    def __getattr__(self, name):
        return self._get_collection(name)

    def __getitem__(self, name):
        return self._get_collection(name)

    def _get_collection(self, name):
        return Collection(self, name)

    def _enable_json_extension(self):
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS json")

    def _list_tables(self):
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
                return [row[0] for row in cursor.fetchall()]

    def _table_exists(self, name):
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", (name,))
                return cursor.fetchone()[0]

    def _create_table(self, name):
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE TABLE {} (id serial PRIMARY KEY, data json);".format(name))

    def _drop_table(self, name):
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS {};".format(name))
