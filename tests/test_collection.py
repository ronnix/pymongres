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

    def test_equality(self):
        from pymongres.collection import Collection
        self.assertEqual(self.db.test, self.db["test"])
        self.assertEqual(self.db.test, Collection(self.db, "test"))

    def test_drop_collection(self):
        from pymongres.collection import Collection

        collection = self.db['test']
        self.assertIsInstance(collection, Collection)
        self.assertIn('test', self.db.collection_names())

        self.db.drop_collection('test')
        self.assertNotIn('test', self.db.collection_names())

        # No exception
        self.db.drop_collection('test')
