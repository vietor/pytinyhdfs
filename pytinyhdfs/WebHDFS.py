# -*- coding: utf-8 -*-
from __future__ import division, with_statement

import os
import sys
import stat
import json
import mmap
import socket

if sys.version_info < (3, 0):
    from urlparse import urlparse
    from urllib import urlencode
    from httplib import HTTPConnection
else:
    long = int
    from urllib.parse import urlparse
    from urllib.parse import urlencode
    from http.client import HTTPConnection


def py2or3str(s):
    if bytes != str:
        if type(s) == bytes:
            return s.decode('utf-8')
    return s


def format_fstatus(i):
    def permission(is_dir, perm):
        dic = {'7': 'rwx', '6': 'rw-', '5': 'r-x', '4': 'r--', '0': '---'}
        return ('d' if is_dir else '-') + ''.join(dic.get(x, x) for x in perm)

    return {
        'type': i["type"],
        'name': i["pathSuffix"],
        'owner': i["owner"],
        'group': i["group"],
        'replication': i["replication"],
        'size': i["length"],
        'modified': long(i["modificationTime"] / 1000),
        'permission': permission(i["type"] == 'DIRECTORY', i["permission"])
    }


class WebHDFS(object):

    def __init__(self, namenode_host, namenode_port, hdfs_username, timeout=10):
        self.host = namenode_host
        self.port = namenode_port
        self.username = hdfs_username
        self.timeout = timeout

    def __pure(self, host, port, method, url, body=None, storeobj=None):
        def isNetworkError(e):
            return isinstance(e, socket.timeout) \
                or (hasattr(e, 'errno') and (e.errno == 10061 or e.errno == 61))

        def renderResponse(response, data):
            error_message = None
            if response.status != 200 and data:
                try:
                    data_dict = json.loads(py2or3str(data))
                    if 'RemoteException' in data_dict:
                        rstatus = data_dict['RemoteException']
                        if 'exception' in rstatus:
                            error_message = rstatus['exception']
                except:
                    pass
            if error_message:
                response.reason = error_message
            return response

        httpClient = None
        try:
            data = None
            httpClient = HTTPConnection(host, port, timeout=self.timeout)
            httpClient.request(method, url, body, headers={})
            response = httpClient.getresponse()
            if not storeobj or response.status != 200:
                data = response.read()
            else:
                storeobj.begin()
                while True:
                    buf = response.read(8192)
                    if not buf:
                        break
                    storeobj.write(buf)
                storeobj.end()
            return renderResponse(response, data), data
        except Exception as e:
            if storeobj:
                storeobj.error(e)
            if not isNetworkError(e):
                raise e
            else:
                raise Exception("Network error, {0}".format(e.strerror or e))
        finally:
            if httpClient:
                httpClient.close()

    def __query(self, method, path, op, query=None):
        url = '/webhdfs/v1{0}?op={1}'.format(path, op)
        if self.username:
            url += '&user.name={0}'.format(self.username)
        if query:
            url += '&{0}'.format(urlencode(query))
        return self.__pure(self.host, self.port, method, url)

    def mkdir(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))
        response, _ = self.__query('PUT', path, 'MKDIRS')
        return response.status, response.reason

    def delete(self, path, recursive=False):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))
        response, _ = self.__query(
            'DELETE', path, 'DELETE', {'recursive': 'true' if recursive else 'false'})
        return response.status, response.reason

    def put(self, data, target_file, replication=1, overwrite=True):
        if os.path.isabs(target_file) == False:
            raise Exception(
                "Only absolute paths supported: %s" % (target_file)
            )
        response, _ = self.__query(
            'PUT', target_file, 'CREATE', {'overwrite': 'true' if overwrite else 'false'})
        if response.status < 300 or response.status >= 400:
            return response.status, response.reason
        result = urlparse(response.msg["location"])
        redirect_host = result.netloc[:result.netloc.index(":")]
        redirect_port = result.netloc[(result.netloc.index(":") + 1):]
        redirect_path = result.path + "?" + result.query + \
            "&replication=" + str(replication)
        response, _ = self.__pure(
            redirect_host, redirect_port, 'PUT', redirect_path, body=data)
        return response.status, response.reason

    def get(self, target_file, storeobj=None):
        if os.path.isabs(target_file) == False:
            raise Exception(
                "Only absolute paths supported: %s" % (target_file)
            )
        data = None
        response, _ = self.__query('GET', target_file, 'OPEN')
        if response.length != None:
            result = urlparse(response.msg["location"])
            redirect_host = result.netloc[:result.netloc.index(":")]
            redirect_port = result.netloc[(result.netloc.index(":") + 1):]
            redirect_path = result.path + "?" + result.query
            response, data = self.__pure(
                redirect_host, redirect_port, 'GET', redirect_path, storeobj=storeobj)
        return response.status, response.reason, data

    def listdir(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))
        files = []
        response, data = self.__query('GET', path, 'LISTSTATUS')
        if data:
            data_dict = json.loads(py2or3str(data))
            if "FileStatuses" in data_dict:
                statuses = data_dict["FileStatuses"]
                for i in statuses["FileStatus"]:
                    files.append(format_fstatus(i))
        return response.status, response.reason, files

    def status(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))
        status = {}
        response, data = self.__query('GET', path, 'GETFILESTATUS')
        if data:
            data_dict = json.loads(py2or3str(data))
            if "FileStatus" in data_dict:
                status = format_fstatus(data_dict["FileStatus"])
        return response.status, response.reason, status

    def putFile(self, local_file, target_file, replication=1, overwrite=True):
        with open(local_file, "rb") as rfile:
            stat = os.fstat(rfile.fileno())
            if stat.st_size < 1:
                file_obj = rfile
            else:
                file_obj = mmap.mmap(
                    rfile.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                return self.put(file_obj, target_file, replication, overwrite)
            finally:
                if stat.st_size > 0:
                    file_obj.close()

    def getFile(self, target_file, local_file):
        class StoreObj(object):

            def __init__(self):
                self._file = None

            def begin(self):
                self._file = open(local_file, "wb")

            def write(self, data):
                self._file.write(data)

            def end(self):
                self._file.close()

            def error(self, e):
                if self._file:
                    self._file.close()
                    self._file = None
                    os.remove(local_file)

        status, reason, _ = self.get(target_file, storeobj=StoreObj())
        return status, reason
