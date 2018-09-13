import base64
import json
import os
import struct
import sys

class ProxyFileServer:
    def __init__(self):
        self.nextHandle = 1
        self.handles = {}
        print("ready")
        while True:
            self.work()

    def work(self):
        command = struct.unpack("16p", sys.stdin.read(16))[0]
        func = {
            "chmod":    self.chmod,
            "create":   self.create,
            "flush":    self.flush,
            "fsync":    self.fsync,
            "link":     self.link,
            "mkdir":    self.mkdir,
            "mknod":    self.mknod,
            "open":     self.open,
            "read":     self.read,
            "readlink": self.readlink,
            "release":  self.release,
            "rename":   self.rename,
            "rmdir":    self.rmdir,
            "statfs":   self.statfs,
            "symlink":  self.symlink,
            "truncate": self.truncate,
            "unlink":   self.unlink,
            "utimens":  self.utimens,
            "write":    self.write,

            "xreaddir": self.xreaddir,
        }[command]

        try:
            self.sendResults(func(self.readParams()))
        except OSError as e:
            self.sendResults({"exception": "OSError", "errno": e.errno})

    def chmod(self, params):
        os.chmod(params["path"], params["mode"])
        return True

    def create(self, params):
        params["flags"] = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        return self.open(params)

    def flush(self, params):
        return os.fsync(self.handles[params["fh"]])

    def fsync(self, params):
        if params["datasync"] != 0:
            return os.fdatasync(self.handles[params["fh"]])
        else:
            return self.flush(params)

    def link(self, params):
        return os.link(params["source"], params["target"])

    def mkdir(self, params):
        return os.mkdir(params["path"], params["mode"])

    def mknod(self, params):
        return os.mknod(params["filename"], params["mode"], params["device"])

    def open(self, params):
        if "mode" in params:
            handle = os.open(params["path"], params["flags"], params["mode"])
        else:
            handle = os.open(params["path"], params["flags"])
        handleID = self.nextHandle
        self.nextHandle += 1
        self.handles[handleID] = handle
        return handleID

    def read(self, params):
        fh = self.handles[params["fh"]]
        os.lseek(fh, params["offset"], 0)
        return os.read(fh, params["size"])

    def readlink(self, params):
        return os.readlink(params["path"])

    def release(self, params):
        fh = self.handles[params["fh"]]
        res = os.close(fh)
        del self.handles[params["fh"]]
        return res

    def rename(self, params):
        return os.rename(params["old"], params["new"])

    def rmdir(self, params):
        return os.rmdir(params["path"])

    def statfs(self, params):
        stv = os.statvfs(params["path"])
        return dict((key, getattr(stv, key)) for key in (
            "f_bavail", "f_bfree", "f_blocks", "f_bsize", "f_favail",
            "f_ffree", "f_files", "f_flag", "f_frsize", "f_namemax"))

    def symlink(self, params):
        return os.symlink(params["source"], params["target"])

    def truncate(self, params):
        with open(params["path"], "r+") as f:
            f.truncate(params["length"])

    def unlink(self, params):
        return os.unlink(params["path"])

    def utimens(self, params):
        return os.utime(params["path"], tuple(params["times"]))

    def write(self, params):
        fh = self.handles[params["fh"]]
        os.lseek(fh, params["offset"], 0)
        return os.write(fh, base64.b64decode(params["data"]))


    def xreaddir(self, params):
        path = params["path"]
        output = {}
        files = ["."] + os.listdir(path)
        for f in files:
            output[f] = {
                "access":     self.computeAccess(path + "/" + f),
                "attributes": self.computeAttributes(path + "/" + f),
            }
        return output

    def computeAccess(self, path):
        access = 0
        if os.access(path, os.R_OK):
            access |= os.R_OK
        if os.access(path, os.W_OK):
            access |= os.W_OK
        if os.access(path, os.X_OK):
            access |= os.X_OK
        return access

    def computeAttributes(self, path):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in (
            "st_atime", "st_ctime", "st_gid", "st_mode", "st_mtime",
            "st_nlink", "st_size", "st_uid"))


    def readParams(self):
        dataLen, isJson = struct.unpack("i?", sys.stdin.read(5))
        if isJson:
            temp = json.loads(sys.stdin.read(dataLen))
            if "path" in temp:
                temp["path"] = "/home/mng/nobackup" + temp["path"]
            return temp
        else:
            return sys.stdin.read(dataLen)

    def sendResults(self, data):
        isJson = False
        serialized = data
        if not isinstance(data, basestring):
            isJson = True
            serialized = json.dumps(data, separators=(',',':'))
        sys.stdout.write(struct.pack("i?", len(serialized), isJson))
        sys.stdout.write(serialized)
