from __future__ import absolute_import

from datetime import datetime

from psycopg2.extensions import QuotedString

from pymongres.errors import InvalidName
from pymongres.json_adapters import Json
from pymongres.resultset import ResultSet


import logging
log = logging.getLogger(__name__)


# Python 3
try:
    unicode
except NameError:
    basestring = unicode = str


class Collection(object):

    def __init__(self, database, name):
        self.database = database
        self.name = name

        self._check_name(name)

        if not self.database._table_exists(name):
            self.database._create_table(name)

    @staticmethod
    def _check_name(name):
        if not isinstance(name, basestring):
            raise TypeError()
        if name == "":
            raise InvalidName("collection names cannot be empty")

    def __eq__(self, other):
        if isinstance(other, Collection):
            return (self.database, self.name) == (other.database, other.name)
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def insert(self, document):
        if isinstance(document, list):
            return self._insert_multi(document)
        else:
            return self._insert_single(document)

    def _insert_multi(self, documents):
        res = []
        for document in documents:
            _id = self._insert_single(document)
            res.append(_id)
        return res

    def _insert_single(self, document):
        with self.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    'INSERT INTO {} (data) VALUES (%s) RETURNING (id)'.format(self.name),
                    [Json(document)]
                )
                _id, = cursor.fetchone()
                return _id

    def find_one(self, query=None):
        sql_query = self._find_query(query)

        with self.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                row = cursor.fetchone()
                if row is None:
                    return None
                else:
                    return self._document_from_row(row)

    def _find_query(self, query):
        sql_query = 'SELECT * FROM {collection}{where}'.format(
            collection=self.name,
            where=self._build_where_clause(query),
        )
        log.debug(sql_query)
        return sql_query

    def _count_query(self, query):
        sql_query = 'SELECT COUNT(*) FROM {collection}{where}'.format(
            collection=self.name,
            where=self._build_where_clause(query),
        )
        log.debug(sql_query)
        return sql_query

    @staticmethod
    def _build_where_clause(query):
        if query is None:
            query = {}

        filters = []
        for key, value in query.items():
            column = Collection._build_where_column(key)
            predicate = Collection._build_where_predicate(value)
            clause = "{} {}".format(column, predicate)
            filters.append(clause)

        if filters:
            return ' WHERE {}'.format('AND'.join(filters))
        else:
            return ''

    @staticmethod
    def _build_where_column(key):
        if key == '_id':
            return "id"
        else:
            return "data->>{}".format(quoted(key))

    @staticmethod
    def _build_where_predicate(value):
        if isinstance(value, dict):
            assert len(value) == 1
            op, value = value.items()[0]
            if op == '$lt':
                return "< {}".format(quoted(value))
            elif op == '$lte':
                return "<= {}".format(quoted(value))
            elif op == '$gt':
                return "> {}".format(quoted(value))
            elif op == '$gte':
                return ">= {}".format(quoted(value))
            else:
                raise Exception("Unsupported operator %s" % op)
        else:
            return "= {}".format(quoted(value))

    @staticmethod
    def _document_from_row(row):
        _id, data = row
        assert '_id' not in data
        data['_id'] = _id
        return data

    def find(self, query=None):
        return ResultSet(self, query)

    def count(self):
        """
        Return the number of documents in the collection
        """

        sql_query = 'SELECT COUNT(*) FROM {}'.format(self.name)

        with self.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                count, = cursor.fetchone()

        return count


def quoted(value, encoding='utf8'):
    if isinstance(value, basestring):
        return QuotedString(value).getquoted().decode(encoding)
    elif isinstance(value, datetime):
        return quoted(value.isoformat())
    else:
        return value
