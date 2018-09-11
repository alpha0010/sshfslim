from fuse import FuseOSError
import json
import os
import struct
import subprocess
from threading import Lock

class ProxyFileClient:
    def __init__(self, server):
        self.proc = subprocess.Popen(
            ["ssh", server, "python", "-i"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE#,
            #stderr=open(os.devnull, "w")
        )

        with open(os.path.dirname(os.path.realpath(__file__)) + "/proxyfileserver.py", "r") as f:
            for line in f:
                if line.strip():
                    self.proc.stdin.write(line)

        self.proc.stdin.write("\nProxyFileServer()\n")

        while self.proc.stdout.readline().strip() != "ready":
            pass

        self.lock = Lock()

    def command(self, command, data):
        with self.lock:
            print "DO: '" + command + "'"
            print "DATA: '" + repr(data) + "'"
            self.sendCommand(command, data)
            res = self.readResults()
            print "RESULTS: '" + repr(res) + "'"
            if isinstance(res, dict) and "exception" in res:
                raise FuseOSError(res["errno"])
            return res

    def sendCommand(self, command, data):
        isJson = False
        serialized = data
        if not isinstance(data, basestring):
            isJson = True
            serialized = json.dumps(data, separators=(',',':'))
        self.proc.stdin.write(struct.pack("16pi?", command, len(serialized), isJson))
        self.proc.stdin.write(serialized)

    def readResults(self):
        dataLen, isJson = struct.unpack("i?", self.proc.stdout.read(5))
        if isJson:
            return json.loads(self.proc.stdout.read(dataLen))
        else:
            return self.proc.stdout.read(dataLen)
