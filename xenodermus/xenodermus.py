import collections
import configparser
import io
import os
import uuid
import sqlite3
import random
import hashlib

from .backends.local import LocalFileStore

class StoredFile:
    chunks = []
    position = 0
    def __init__(self, chunks, size):
        self.size = size
        self.chunks = chunks

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for f in self.chunks:
            try:
                f.close()
            except:
                pass
        return

    def read(self, size=-1):
        data = io.BytesIO()
        left = size
        for c in self.chunks:
            if left == 0:
                break
            part = c.read(left)
            if not part:
                continue
            data.write(part)
            if size != -1:
                left -= len(part)
        data.seek(0)
        d = data.read()
        self.position += len(d)
        return d

    def seek(self, offset, from_where=0):
        left = offset
        if from_where == 0:
            if offset > self.position:
                left = offset - self.position
            else:
                for c in self.chunks:
                    c.seek(0,0)
                self.position = 0
        elif from_where == 1:
            pass
        elif from_where == 2:
            offset = self.size - offset
            if offset < self.position:
                self.seek(0,0)
                left = offset
            else:
                left = offset - self.position
        else:
            raise NotImplementedError
        for c in self.chunks:
            if left == 0:
                break
            start = c.tell()
            c.seek(0, 2)
            end = c.tell()
            if end - start > left:
                c.seek(start + left, 0)
                end = c.tell()
            left -= (end - start)
            self.position += (end - start)

    def tell(self):
        return self.position

class Hoard(collections.MutableMapping):
    """A file store."""

    config = configparser.ConfigParser(delimiters=('=',))
    config.optionxform = lambda o: o # need to be case-sensitive
    hoard_id = None
    chunk_stores = {}
    con = None

    def __init__(self, hoard_id=None, path='hoard', config=None,
                 chunk_size=256*1024, chunk_stores=[]):
        if config:
            self.config_path = config
            self.config.read(config)
            for store in self.config['STORES']:
                if self.config['STORES'][store] == 'local':
                    s = LocalFileStore(config=os.path.join(os.path.dirname(config), store))
                    self.chunk_stores[s.config['STORE']['store_id']] = s
                else:
                    raise ValueError("Invalid store type.")
        else:
            self.config['HOARD'] = {}
            self.config['STORES'] = {}
            self.config['BALANCE'] = {}
            self.config['HOARD']['path'] = path
            self.config['HOARD']['hoard_id'] = hoard_id if hoard_id else uuid.uuid4().hex
            self.config['HOARD']['chunk_size'] = str(chunk_size)
            self.config['HOARD']['db'] = 'sqlite'
            self.config['HOARD']['db_path'] = self.config['HOARD']['hoard_id'] + '.db'
            self.config['HOARD']['allow_duplicates'] = 'false'
            if not chunk_stores:
                chunk_stores = [LocalFileStore()]
            for store in chunk_stores:
                self.chunk_stores[store.config['STORE']['store_id']] = store
                self.config['STORES'][os.path.join(store.config['STORE']['path'], 'store.conf')] = store.config['STORE']['type']
                self.config['BALANCE'][store.config['STORE']['store_id']] = '1'
            self.config_path = os.path.join(self.config['HOARD']['path'], 'hoard.conf')
        if not os.path.exists(self.config['HOARD']['path']):
            os.makedirs(self.config['HOARD']['path'])
        with open(self.config_path, 'w') as f:
            self.config.write(f)

        if self.config['HOARD']['db'] == 'sqlite':
            path = os.path.join(os.path.dirname(self.config_path), self.config['HOARD']['db_path'])
            self.con = sqlite3.connect(path, check_same_thread=False)
        else:
            raise ValueError('Invalid DB type.')

        self.initialize_db()

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        with self.con as con:
            cur = con.cursor()
            con.execute("SELECT COUNT(*) from file;")
            return cur.fetchone()[0]

    def __getitem__(self, key):
        chunks = []
        with self.con as con:
            cur = con.cursor()
            cur.execute("SELECT size FROM file WHERE id = ?;", (key,))
            r = cur.fetchone()
            if not r:
                raise KeyError("No such file in this Hoard.")
            size = r[0]
            cur.execute("""
                SELECT chunk_store, name
                FROM chunk
                WHERE file_id = ?
                ORDER BY ordering ASC;""", (key,))
            for c in cur.fetchall():
                chunks.append(self.chunk_stores[c[0]][c[1]])
            if not chunks:
                raise KeyError("No such file in this Hoard.")
            return StoredFile(chunks, size)

    def __setitem__(self, key, value):
        raise NotImplementedError("Use Hoard.put().")

    def __delitem__(self, key):
        with self.con as con:
            cur = con.cursor()
            cur.execute("""
                SELECT chunk_store, name
                FROM chunk
                WHERE file_id = ?
                ORDER BY ordering ASC;""", (key,))
            for c in cur.fetchall():
                del self.chunk_stores[c[0]][c[1]]
            cur.execute("""
                DELETE
                FROM chunk
                WHERE file_id = ?;""", (key,))
            cur.execute("""
                DELETE
                FROM file
                WHERE id = ?;""", (key,))

    def put(self, data, name=None):
        name = name
        size = 0
        file_hash = hashlib.md5()
        while True:
            part = data.read(2**16)
            size += len(part)
            if not part:
                break
            file_hash.update(part)
        file_hash = file_hash.hexdigest()
        if not self.config.getboolean('HOARD', 'allow_duplicates'):
            with self.con as con:
                cur = con.cursor()
                cur.execute("SELECT id FROM file WHERE size = ? AND hash = ?;", (size, file_hash))
                r = cur.fetchone()
                if r:
                    return r[0]
        data.seek(0)
        file_id = None
        with self.con as con:
            cur = con.cursor()
            cur.execute("INSERT INTO file (name, size, hash) VALUES (?, ?, ?);", (name, size, file_hash))
            file_id = cur.lastrowid
            ordering = 1
            while True:
                name = uuid.uuid4().hex
                chunk_data = data.read(int(self.config['HOARD']['chunk_size']))
                if not chunk_data:
                    break
                store_id = self.get_store_id()
                cur.execute("INSERT INTO chunk (file_id, ordering, name, chunk_store) VALUES (?, ?, ?, ?);", (file_id, ordering, name, store_id))
                self.chunk_stores[store_id][name] = chunk_data
                ordering += 1
        return file_id

    def get_store_id(self):
        """Returns a random store ID according to the assigned weights."""
        total = sum(int(v) for v in self.config['BALANCE'].values())
        r = random.uniform(0, total)
        cur = 0
        for s, b in self.config['BALANCE'].items():
            if cur + int(b) >= r:
                return s
            cur += int(b)

    def initialize_db(self):
        with self.con as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS file (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    size INTEGER,
                    hash TEXT);
            """)
            con.execute("""
                CREATE INDEX IF NOT EXISTS file_size_hash_idx
                    ON file (size, hash);
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS chunk (
                    id INTEGER PRIMARY KEY,
                    file_id INTEGER NOT NULL,
                    ordering INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    chunk_store TEXT NOT NULL,
                    FOREIGN KEY(file_id) REFERENCES file(id) );
            """)
            con.execute("""
                CREATE INDEX IF NOT EXISTS chunk_file_order_idx
                    ON chunk (file_id, ordering, name);
            """)
