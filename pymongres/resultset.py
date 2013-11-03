# coding: utf-8

# Copyright 2013 Ronan Amicel
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import


class ResultSet(object):

    def __init__(self, collection, spec, fields, order_by=None):
        self.collection = collection
        self.spec = spec
        self.fields = fields
        self.order_by = order_by

    def __iter__(self):
        sql_query = self.collection._find_query(self.spec, self.order_by)

        with self.collection.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                for row in iter(cursor.fetchone, None):
                    yield self.collection._document_from_row(row, fields=self.fields)

    def __next__(self):
        return next(self.__iter__())
    next = __next__

    def count(self):
        sql_query = self.collection._count_query(self.spec)

        with self.collection.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                count, = cursor.fetchone()

        return count

    def sort(self, key):
        return ResultSet(
            collection=self.collection,
            spec=self.spec,
            fields=self.fields,
            order_by=key,
        )
