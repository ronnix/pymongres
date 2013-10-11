from __future__ import absolute_import

import unittest


class TestCollectionName(unittest.TestCase):

    def setUp(self):
        from pymongres import MongresClient
        self.client = MongresClient()
        self.db = self.client.pymongres_test

    def test_non_string_name(self):
        from pymongres.collection import Collection
        self.assertRaises(TypeError, Collection, self.db, 5)

    def test_empty_name(self):
        from pymongres.collection import Collection
        from pymongres.errors import InvalidName
        self.assertRaises(InvalidName, Collection, self.db, "")
