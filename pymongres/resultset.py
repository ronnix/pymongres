from __future__ import absolute_import


class ResultSet(object):

    def __init__(self, collection, query):
        self.collection = collection
        self.query = query

    def __iter__(self):
        sql_query = self.collection._find_query(self.query)

        with self.collection.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                for row in iter(cursor.fetchone, None):
                    yield self.collection._document_from_row(row)

    def count(self):
        sql_query = self.collection._count_query(self.query)

        with self.collection.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                count, = cursor.fetchone()

        return count

    def sort(self, key):
        return self  # FIXME
