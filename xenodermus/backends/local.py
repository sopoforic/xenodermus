import os
import uuid
import configparser

class LocalFileStore:
    """A chunk store backed by a locally-accessible file system."""

    config = configparser.ConfigParser(delimiters=('=',))
    config.optionxform = lambda o: o # need to be case-sensitive

    def get_path(self, key):
        if len(key) != 32:
            raise ValueError("Keys must be 32-character strings.")
        return os.path.join(os.path.dirname(self.config_path), key[:2], key[2:4], key)

    def __init__(self, base_path='stores', store_id=None, path=None, config=None, *args, **kwargs):
        if config:
            self.config_path = config
            self.config.read(config)
        else:
            self.config['STORE'] = {}
            self.config['STORE']['type'] = 'local'
            self.config['STORE']['base_path'] = base_path
            self.config['STORE']['store_id'] = store_id if store_id else uuid.uuid4().hex
            self.config['STORE']['path'] = path if path else os.path.join(self.config['STORE']['base_path'], self.config['STORE']['store_id'], '')
            self.config_path = os.path.join(self.config['STORE']['path'], 'store.conf')
        if not os.path.exists(self.config['STORE']['path']):
            os.makedirs(self.config['STORE']['path'])
        with open(os.path.join(self.config['STORE']['path'], 'store.conf'), 'w') as f:
            self.config.write(f)

    def __getitem__(self, key):
        filepath = self.get_path(key)
        return open(filepath, 'rb')

    def __setitem__(self, key, value):
        filepath = self.get_path(key)
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))
        with open(filepath, 'wb') as f:
            return f.write(value)

    def __delitem__(self, key):
        filepath = self.get_path(key)
        os.remove(filepath)
