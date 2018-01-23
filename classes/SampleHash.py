import hashlib
from pathlib import Path
import logging
import os
import platform
import time
import multiprocessing


class BasicHash:
    BLAKE2B = hashlib.blake2b
    SHA256 = hashlib.sha256
    MD5 = hashlib.md5

    def __init__(self, file: str, algo=BLAKE2B) -> None:
        if platform.system() == "Windows":
            self.file_to_hash = file
        else:
            self.file_to_hash = file
        self.algo = algo
        self.read_buffer = 2**16
        try:
            self.file_size = os.path.getsize(self.file_to_hash)
        except FileNotFoundError as e:
            raise FileNotFoundError(e)

    def do_hash(self):
        pass


class SampleHash(BasicHash):
    def __init__(self, file: str, algo=BasicHash.BLAKE2B) -> None:
        super().__init__(file, algo)
        try:
            self.bytes_to_sample = 3*1000**2
            self.megabyte = 1000**2
            self.chunk_size = int(self.bytes_to_sample / 10)
            self.mini_chunks = int(self.chunk_size / 4000)
            self.end = int(3)
        except FileNotFoundError as e:
            raise FileNotFoundError(e)

    def do_hash(self, full_only=False):
        if full_only:
            return self._full_hash()

        if int((self.file_size / self.megabyte)) > self.end:
            return self._quick_hash()
        else:
            return self._full_hash()

    def _quick_hash(self):
        hash_l = self.get_hasher()
        with open(str(self.file_to_hash), "rb") as f:
            chunk_list = self._chunk_list()
            for p in chunk_list:
                f.seek(p)
                for ch in range(self.mini_chunks):
                    hash_l.update(f.read(4000))
        return hash_l.hexdigest()

    def _quick_hash_front_read(self):
        hash_l = self.get_hasher()
        end = self.bytes_to_sample
        with open(str(self.file_to_hash), "rb") as f:
            while f.tell() < end:
                hash_l.update(f.read(4000))
        return hash_l.hexdigest()

    def _full_hash(self):
        hash_l = self.get_hasher()
        for p in self._lazy_read(open(str(self.file_to_hash), "rb")):
            hash_l.update(p)
        return hash_l.hexdigest()

    def _lazy_read(self, file_object):
        while True:
            data = file_object.read(self.read_buffer)
            if not data:
                break
            yield data

    def _chunk_list(self):
        fs = self.file_size
        positions = []
        for i in range(0, 100, 10):
            positions.append(int(fs*(i/100)))
        return positions

    def get_hasher(self):
        if self.algo is hashlib.md5:
            return hashlib.md5()
        if self.algo is hashlib.sha256:
            return hashlib.sha256()
        if self.algo is hashlib.sha1:
            return hashlib.sha1()
        if self.algo is hashlib.sha224:
            return hashlib.sha224()
        if self.algo is hashlib.sha384:
            return hashlib.sha384()
        if self.algo is hashlib.sha512:
            return hashlib.sha512()
        if self.algo is hashlib.blake2b:
            return hashlib.blake2b(digest_size=16)


class ThreadedHash:
    def __init__(self, digest_type=hashlib.blake2b, digest_size=16):
        self.digest_type = digest_type
        self.digest_size = digest_size

    def hash_file(self, path):
        sh = SampleHash(path)
        return sh.do_hash(), path

    def run_process(self, li):
        '''
        :type li: dict
        :param li: type dict
        :return:
        '''
        pool = multiprocessing.Pool(processes=len(li))
        items = pool.map_async(self.hash_file, li).get()
        pool.close()
        return items