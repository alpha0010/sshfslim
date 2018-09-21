#!/usr/bin/env python

import base64
import logging
import os.path

from errno import EACCES
from fuse import FUSE, FuseOSError, Operations
from lru import LRUCacheDict
from metadatavfs import MetadataVFS
from proxyfileclient import ProxyFileClient

class SSHFSlim(Operations):
    def __init__(self, server):
        self.client = ProxyFileClient(server)
        self.vfs = MetadataVFS(self.client)
        self.cache = LRUCacheDict(max_size=1024, expiration=60 * 10, concurrent=True)

    def __call__(self, op, path, *args):
        return super(SSHFSlim, self).__call__(op, path, *args)

    def access(self, path, mode):
        if not self.vfs.access(path, mode):
            raise FuseOSError(EACCES)

    def chmod(self, path, mode):
        return self.client.command("chmod", {"path": path, "mode": mode})

    def create(self, path, mode):
        result = self.client.command("create", {"path": path, "mode": mode})
        self.vfs.invalidate(path)
        return result

    def flush(self, path, fh):
        return self.client.command("flush", {"fh": fh})

    def fsync(self, path, datasync, fh):
        return self.client.command("fsync", {"datasync": datasync, "fh": fh})

    def getattr(self, path, fh=None):
        return self.vfs.getattr(path)

    getxattr = None

    def link(self, target, source):
        result = self.client.command("link", {"source": source, "target": target})
        self.vfs.invalidate(target)
        return result

    listxattr = None

    def mkdir(self, path, mode):
        result = self.client.command("mkdir", {"path": path, "mode": mode})
        self.vfs.invalidate(path)
        return result

    def mknod(self, filename, mode, device):
        result = self.client.command("mknod", {"filename": filename, "mode": mode, "device": device})
        self.vfs.invalidate(filename)
        return result

    def open(self, path, flags):
        return self.client.command("open", {"path": path, "flags": flags})

    def read(self, path, size, offset, fh):
        return self.client.command("read", {"fh": fh, "offset": offset, "size": size})

    def readdir(self, path, fh):
        return self.vfs.readdir(path)

    def readlink(self, path):
        result = None
        try:
            result = self.cache["readlink:" + path]
        except KeyError:
            result = self.client.command("readlink", {"path": path})
            self.cache["readlink:" + path] = result
        return result

    def release(self, path, fh):
        return self.client.command("release", {"fh": fh})

    def rename(self, old, new):
        result = self.client.command("rename", {"old": old, "new": new})
        self.vfs.invalidate(old)
        self.vfs.invalidate(new)
        return result

    def rmdir(self, path):
        result = self.client.command("rmdir", {"path": path})
        self.vfs.invalidate(path)
        return result

    def statfs(self, path):
        result = None
        try:
            result = self.cache["statfs:" + path]
        except KeyError:
            result = self.client.command("statfs", {"path": path})
            self.cache["statfs:" + path] = result
        return result

    def symlink(self, target, source):
        result = self.client.command("symlink", {"target": target, "source": source})
        self.vfs.invalidate(target)
        return result

    def truncate(self, path, length, fh=None):
        return self.client.command("truncate", {"path": path, "length": length})

    def unlink(self, path):
        result = self.client.command("unlink", {"path": path})
        self.vfs.invalidate(path)
        return result

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
