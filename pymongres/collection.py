from __future__ import absolute_import

from datetime import datetime

from six import iteritems

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

    def find_one(self, spec=None, fields=None):
        sql_query = self._find_query(spec)

        with self.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                row = cursor.fetchone()
                if row is None:
                    return None
                else:
                    return self._document_from_row(row, fields=fields)

    def _find_query(self, spec, order_by=None):
        sql_query = 'SELECT * FROM {collection}{where}{sort}'.format(
            collection=self.name,
            where=self._build_where_clause(spec),
            sort=self._build_order_by_clause(order_by),
        )
        log.debug(sql_query)
        return sql_query

    def _count_query(self, spec):
        sql_query = 'SELECT COUNT(*) FROM {collection}{where}'.format(
            collection=self.name,
            where=self._build_where_clause(spec),
        )
        log.debug(sql_query)
        return sql_query

    @staticmethod
    def _build_where_clause(spec):
        if spec is None:
            spec = {}

        filters = []
        for key, value in spec.items():
            column = Collection._build_column(key)
            predicate = Collection._build_where_predicate(value)
            clause = "{} {}".format(column, predicate)
            filters.append(clause)

        if filters:
            return ' WHERE {}'.format('AND'.join(filters))
        else:
            return ''

    @staticmethod
    def _build_column(key):
        if key == '_id':
            return "id"
        else:
            return "data->>{}".format(quoted(key))

    @staticmethod
    def _build_where_predicate(value):
        if isinstance(value, dict):
            assert len(value) == 1
            op, value = next(iteritems(value))
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
    def _build_order_by_clause(key):
        if key is None:
            return ''
        column = Collection._build_column(key)
        return ' ORDER BY {}'.format(column)

    @staticmethod
    def _document_from_row(row, fields=None):
        _id, data = row
        return Collection._filter_document(_id, data, fields)

    @staticmethod
    def _filter_document(_id, data, fields=None):
        if fields is None:
            data['_id'] = _id
            return data
        elif isinstance(fields, dict):
            return Collection._filter_document_by_dict(_id, data, fields)
        else:
            return Collection._filter_document_by_list(_id, data, fields)

    @staticmethod
    def _filter_document_by_list(_id, data, fields=None):
        """
        Exclusive list of fields to include

        Note: _id is always included in this case
        """
        res = {}
        for field in fields:
            if field == '_id':
                continue
            path = field.split('.')
            source, target = data, res
            if len(path) > 1:
                for item in path[:-1]:
                    source = source[item]
                    target = target.setdefault(item, {})
            name = path[-1]
            target[name] = source[name]
        res['_id'] = _id
        return res

    @staticmethod
    def _filter_document_by_dict(_id, data, fields=None):
        """
        Exclusive list of fields to include
        """
        data['_id'] = _id
        for field, include in iteritems(fields):
            if not include:
                path = field.split('.')
                target = data
                if len(path) > 1:
                    for item in path[:-1]:
                        target = target[item]
                name = path[-1]
                del target[name]
        return data

    def find(self, spec=None, fields=None):
        return ResultSet(self, spec, fields)

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
