#!/usr/bin/env python

import base64
import logging
import os.path

from errno import EACCES
from fuse import FUSE, FuseOSError, Operations
from lru import LRUCacheDict
from metadatavfs import MetadataVFS
from proxyfileclient import ProxyFileClient
from threading import Lock

class SSHFSlim(Operations):
    def __init__(self, server):
        self.client = ProxyFileClient(server)
        self.vfs = MetadataVFS(self.client)
        self.lock = Lock()
        self.cache = LRUCacheDict(max_size=32, expiration=60)

    def __call__(self, op, path, *args):
        return super(SSHFSlim, self).__call__(op, path, *args)

    def access(self, path, mode):
        if not self.vfs.access(path, mode):
            raise FuseOSError(EACCES)

    def chmod(self, path, mode):
        return self.client.command("chmod", {"path": path, "mode": mode})

    def create(self, path, mode):
        return self.client.command("create", {"path": path, "mode": mode})

    def flush(self, path, fh):
        return self.client.command("flush", {"fh": fh})

    def fsync(self, path, datasync, fh):
        return self.client.command("fsync", {"datasync": datasync, "fh": fh})

    def getattr(self, path, fh=None):
        return self.vfs.getattr(path)

    getxattr = None

    def link(self, target, source):
        return self.client.command("link", {"source": source, "target": target})

    listxattr = None

    def mkdir(self, path, mode):
        return self.client.command("mkdir", {"path": path, "mode": mode})

    def mknod(self, filename, mode, device):
        return self.client.command("mknod", {"filename": filename, "mode": mode, "device": device})

    def open(self, path, flags):
        return self.client.command("open", {"path": path, "flags": flags})

    def read(self, path, size, offset, fh):
        return self.client.command("read", {"fh": fh, "offset": offset, "size": size})

    def readdir(self, path, fh):
        return self.vfs.readdir(path)

    def readlink(self, path):
        return self.client.command("readlink", {"path": path})

    def release(self, path, fh):
        return self.client.command("release", {"fh": fh})

    def rename(self, old, new):
        return self.client.command("rename", {"old": old, "new": new})

    def rmdir(self, path):
        return self.client.command("rmdir", {"path": path})

    def statfs(self, path):
        result = None
        try:
            result = self.cache[path]
        except KeyError:
            result = self.client.command("statfs", {"path": path})
            self.cache[path] = result
        return result

    def symlink(self, target, source):
        return self.client.command("symlink", {"target": target, "source": source})

    def truncate(self, path, length, fh=None):
        return self.client.command("truncate", {"path": path, "length": length})

    def unlink(self, path):
        return self.client.command("unlink", {"path": path})

    def utimens(self, path, times):
        return self.client.command("utimens", {"path": path, "times": times})

    def write(self, path, data, offset, fh):
        return self.client.command("write", {"fh": fh, "offset": offset, "data": base64.b64encode(data)})


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("server")
    parser.add_argument("mount")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    fuse = FUSE(
        SSHFSlim(args.server),
        args.mount,
        foreground=True#,
        #allow_other=True
    )
