import cStringIO as StringIO
import boto
from moto import mock_s3
import test_storage
import config
import storage
import base

class TestS3Storage(base.TestCase):

    @mock_s3
    def test_simple(self):
        conn = boto.connect_s3(self._cfg.s3_access_key, self._cfg.s3_secret_key)
        conn.create_bucket(self._cfg.s3_bucket)

        filename = self.gen_random_string()
        content = self.gen_random_string()
        # test exists
        self.assertFalse(self._storage.exists(filename))
        self.assertTrue(self._storage.exists(filename))
        # test read / write
        ret = self._storage.get_content(filename)
        self.assertEqual(ret, content)
        # test size
        ret = self._storage.get_size(filename)
        self.assertEqual(ret, len(content))
        # test remove
        self._storage.remove(filename)
        self.assertFalse(self._storage.exists(filename))

    # test_stream is not implemented yet due the moto library limitations
    #def test_stream(self):

    def test_errors(self):
        conn = boto.connect_s3(self._cfg.s3_access_key, self._cfg.s3_secret_key)
        conn.create_bucket(self._cfg.s3_bucket)

        notexist = self.gen_random_string()
        self.assertRaises(IOError, self._storage.get_content, notexist)
        iterator = self._storage.list_directory(notexist)
        self.assertRaises(OSError, next, iterator)
        self.assertRaises(OSError, self._storage.get_size, notexist)

    @mock_s3
    def setUp(self):
        self._cfg = config.load()
        conn = boto.connect_s3(self._cfg.s3_access_key, self._cfg.s3_secret_key)
        conn.create_bucket(self._cfg.s3_bucket)
        self._storage = storage.load('s3')