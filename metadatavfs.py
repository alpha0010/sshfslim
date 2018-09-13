from errno import ENOENT
from fuse import FuseOSError
from lru import LRUCacheDict
import os.path
from threading import Lock

class MetadataVFS:
    def __init__(self, client):
        self.client = client
        self.lock = Lock()
        self.cache = LRUCacheDict(max_size=32, expiration=60)

    def access(self, path, mode):
        dirName, fileName = os.path.split(path)
        if not fileName:
            fileName = "."
        dirData = self.xreaddir(dirName)
        return fileName in dirData and (mode == 0 or dirData[fileName]["access"] & mode)

    def getattr(self, path):
        dirName, fileName = os.path.split(path)
        if not fileName:
            fileName = "."
        dirData = self.xreaddir(dirName)
        if fileName in dirData:
            return dirData[fileName]["attributes"]
        raise FuseOSError(ENOENT)

    def readdir(self, path):
        return [".."] + list(self.xreaddir(path).keys())

    def xreaddir(self, path):
        with self.lock:
            dirData = None
            try:
                dirData = self.cache[path]
            except KeyError:
                dirData = self.client.command("xreaddir", {"path": path})
                self.cache[path] = dirData
            return dirData
