from __future__ import absolute_import

from pymongres.database import Database


class MongresClient(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __getattr__(self, name):
        return self._get_database(name)

    def __getitem__(self, name):
        return self._get_database(name)

    def _get_database(self, name):
        return Database(self, name)
