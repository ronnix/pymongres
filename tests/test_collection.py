from __future__ import absolute_import

from six import iterkeys
from six.moves import xrange

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

    def test_field_selection(self):
        db = self.db
        db.drop_collection("test")

        doc = {"a": 1, "b": 5, "c": {"d": 5, "e": 10}}
        db.test.insert(doc)

        # Test field inclusion

        doc = next(db.test.find({}, ["_id"]))
        self.assertEqual(list(iterkeys(doc)), ["_id"])

        doc = next(db.test.find({}, ["a"]))
        l = sorted(list(iterkeys(doc)))
        self.assertEqual(l, ["_id", "a"])

        doc = next(db.test.find({}, ["b"]))
        l = sorted(list(iterkeys(doc)))
        self.assertEqual(l, ["_id", "b"])

        doc = next(db.test.find({}, ["c"]))
        l = sorted(list(iterkeys(doc)))
        self.assertEqual(l, ["_id", "c"])

        doc = next(db.test.find({}, ["a"]))
        self.assertEqual(doc["a"], 1)

        doc = next(db.test.find({}, ["b"]))
        self.assertEqual(doc["b"], 5)

        doc = next(db.test.find({}, ["c"]))
        self.assertEqual(doc["c"], {"d": 5, "e": 10})

        # Test inclusion of fields with dots

        doc = next(db.test.find({}, ["c.d"]))
        self.assertEqual(doc["c"], {"d": 5})

        doc = next(db.test.find({}, ["c.e"]))
        self.assertEqual(doc["c"], {"e": 10})

        doc = next(db.test.find({}, ["b", "c.e"]))
        self.assertEqual(doc["c"], {"e": 10})

        doc = next(db.test.find({}, ["b", "c.e"]))
        l = sorted(list(iterkeys(doc)))
        self.assertEqual(l, ["_id", "b", "c"])

        doc = next(db.test.find({}, ["b", "c.e"]))
        self.assertEqual(doc["b"], 5)

        # Test field exclusion

        doc = next(db.test.find({}, {"a": False, "b": 0}))
        l = sorted(list(iterkeys(doc)))
        self.assertEqual(l, ["_id", "c"])

        doc = next(db.test.find({}, {"_id": False}))
        self.assertNotIn("_id", iterkeys(doc))
