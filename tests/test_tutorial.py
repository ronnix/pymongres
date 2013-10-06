from __future__ import absolute_import

from datetime import datetime
import unittest


class TestImport(unittest.TestCase):

    def test_import(self):
        import pymongres


class TestClient(unittest.TestCase):

    def test_create_client(self):
        from pymongres import MongresClient
        client = MongresClient()

    # def test_create_client_with_host(self):
    #     from pymongres import MongresClient
    #     client = MongresClient('localhost', 5432)

    # def test_create_client_with_url(self):
    #     from pymongres import MongresClient
    #     client = MongresClient('postgresql://localhost:5432/')


class TestDatabase(unittest.TestCase):

    def setUp(self):
        from pymongres import MongresClient
        self.client = MongresClient()

    def test_database_as_attribute(self):
        db = self.client.test_database

    def test_database_as_key(self):
        db = self.client['test_database']


class TestCollections(unittest.TestCase):

    def setUp(self):
        from pymongres import MongresClient
        client = MongresClient()
        self.db = client.test_database

    def test_collection_as_attribute(self):
        collection = self.db.test_collection

    def test_collection_as_key(self):
        collection = self.db['test_collection']

    def test_collection_names(self):
        names = self.db.collection_names()
        self.assertIsInstance(names, list)


class TestInsertDocument(unittest.TestCase):

    def setUp(self):
        from pymongres import MongresClient
        client = MongresClient()
        self.posts = client.test_database.posts

    def tearDown(self):
        with self.posts.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('DELETE FROM posts')

    def test_insert_document(self):
        post = {
            "author": "Mike",
            "text": "My first blog post!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.utcnow(),
        }
        post_id = self.posts.insert(post)


class TestFindOne(unittest.TestCase):

    def setUp(self):
        from pymongres import MongresClient
        client = MongresClient()
        self.posts = client.test_database.posts
        self.post_id = self.posts.insert({
            "author": "Mike",
            "text": "My first blog post!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.utcnow(),
        })

    def tearDown(self):
        with self.posts.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('DELETE FROM posts')

    def test_find_one_no_args(self):
        document = self.posts.find_one()
        self.assertIn("_id", document.keys())

    def test_find_one_by_field(self):
        document = self.posts.find_one({"author": "Mike"})
        self.assertIn("_id", document.keys())
        self.assertEqual(self.post_id, document["_id"])
        self.assertEqual("Mike", document['author'])

    def test_find_one_by_field_no_result(self):
        document = self.posts.find_one({"author": "Eliot"})
        self.assertIsNone(document)

    def test_find_one_by_id(self):
        document = self.posts.find_one({"_id": self.post_id})
        self.assertEqual(self.post_id, document["_id"])


class TestBulkInsert(unittest.TestCase):

    def setUp(self):
        from pymongres import MongresClient
        client = MongresClient()
        self.posts = client.test_database.posts

    def tearDown(self):
        with self.posts.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('DELETE FROM posts')

    def test_bulk_insert(self):
        new_posts = [
            {
                "author": "Mike",
                "text": "Another post!",
                "tags": ["bulk", "insert"],
                "date": datetime(2009, 11, 12, 11, 14)
            },
            {
                "author": "Eliot",
                "title": "MongoDB is fun",
                "text": "and pretty easy too!",
                "date": datetime(2009, 11, 10, 10, 45)
            }
        ]
        post_ids = self.posts.insert(new_posts)
        self.assertEqual(2, len(post_ids))


class TestFind(unittest.TestCase):

    def setUp(self):
        from pymongres import MongresClient
        client = MongresClient()
        self.posts = client.test_database.posts
        self.posts.insert([
            {
                "author": "Mike",
                "text": "Another post!",
                "tags": ["bulk", "insert"],
                "date": datetime(2009, 11, 12, 11, 14)
            },
            {
                "author": "Eliot",
                "title": "MongoDB is fun",
                "text": "and pretty easy too!",
                "date": datetime(2009, 11, 10, 10, 45)
            }
        ])

    def tearDown(self):
        with self.posts.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('DELETE FROM posts')

    def test_find_no_args(self):
        posts = list(self.posts.find())
        self.assertEquals(2, len(posts))

    def test_find_by_field(self):
        posts = list(self.posts.find({"author": "Mike"}))
        self.assertEquals(1, len(posts))


class TestCount(unittest.TestCase):

    def setUp(self):
        from pymongres import MongresClient
        client = MongresClient()
        self.posts = client.test_database.posts
        self.posts.insert([
            {
                "author": "Mike",
                "text": "Another post!",
                "tags": ["bulk", "insert"],
                "date": datetime(2009, 11, 12, 11, 14)
            },
            {
                "author": "Eliot",
                "title": "MongoDB is fun",
                "text": "and pretty easy too!",
                "date": datetime(2009, 11, 10, 10, 45)
            }
        ])

    def tearDown(self):
        with self.posts.database.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('DELETE FROM posts')

    def test_count_collection(self):
        self.assertEquals(2, self.posts.count())

    def test_count_result_set(self):
        count = self.posts.find({"author": "Mike"}).count()
        self.assertEquals(1, count)
