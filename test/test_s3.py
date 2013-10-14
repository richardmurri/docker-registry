import base
import boto
import config
import moto
import storage


class TestS3Storage(base.TestCase):

    @moto.mock_s3
    def test_simple(self):
        conn = boto.connect_s3(self._cfg.s3_access_key,
                               self._cfg.s3_secret_key)
        conn.create_bucket(self._cfg.s3_bucket)

        filename = self.gen_random_string()
        content = self.gen_random_string()
        # test exists
        self.assertFalse(self._storage.exists(filename))
        self._storage.put_content(filename, content)
        self.assertTrue(self._storage.exists(filename))
        # test read / write
        ret = self._storage.get_content(filename)
        self.assertEqual(ret, content)
        # test size
        ret = self.get_size(filename)
        self.assertEqual(ret, len(content))
        # test remove
        self._storage.remove(filename)
        self.assertFalse(self._storage.exists(filename))

    # test_stream is not implemented yet due the moto library limitations
    #def test_stream(self):

    @moto.mock_s3
    def test_errors(self):
        conn = boto.connect_s3(self._cfg.s3_access_key,
                               self._cfg.s3_secret_key)
        conn.create_bucket(self._cfg.s3_bucket)

        notexist = self.gen_random_string()
        self.assertRaises(IOError, self._storage.get_content, notexist)
        iterator = self._storage.list_directory(notexist)
        self.assertRaises(OSError, next, iterator)
        self.assertRaises(OSError, self.get_size, notexist)

    @moto.mock_s3
    def setUp(self):
        self._cfg = config.load()
        self.check_env_variables()
        self._cfg.s3_bucket = self._cfg.s3_bucket.lower()
        conn = boto.connect_s3(self._cfg.s3_access_key,
                               self._cfg.s3_secret_key)
        conn.create_bucket(self._cfg.s3_bucket)
        self._storage = storage.load('s3', self._cfg)

    def check_env_variables(self):
        if self._cfg.s3_bucket == "!ENV_NOT_FOUND":
            self._cfg.s3_bucket = "s3_bucket"
        if self._cfg.s3_access_key == "!ENV_NOT_FOUND":
            self._cfg.s3_access_key = "s3_access_key"
        if self._cfg.s3_secret_key == "!ENV_NOT_FOUND":
            self._cfg.s3_secret_key = "s3_secret_key"

    def get_size(self, path):
        """Patched version of get_size() method in order to work with moto."""

        path = self._storage._init_path(path)
        # Lookup does a HEAD HTTP Request on the object
        key = self._storage._s3_bucket.lookup(path)
        if not key:
            raise OSError('No such key: \'{0}\''.format(path))
        else:
            # Little trick to retrieve the size correctly with moto
            key.get_contents_as_string()
        return key.size
