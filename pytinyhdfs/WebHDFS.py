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
    from urllib.parse import urlparse
    from urllib.parse import urlencode
    from http.client import HTTPConnection


def py2or3str(s):
    if bytes != str:
        if type(s) == bytes:
            return s.decode('utf-8')
    return s


def format_length(size):
    def ffloat(v):
        return '{:0.2f}'.format(v).rstrip('0').rstrip('.')

    KB = 1024
    MB = 1024 * 1024
    GB = 1024 * 1024 * 1024
    if size < KB:
        return str(size) + ' B'
    elif size < MB:
        return ffloat(size / KB) + ' KB'
    elif size < GB:
        return ffloat(size / MB) + ' MB'
    else:
        return ffloat(size / GB) + ' GB'


def format_permission(is_dir, perm):
    dic = {'7': 'rwx', '6': 'rw-', '5': 'r-x', '4': 'r--', '0': '---'}
    return ('d' if is_dir else '-') + ''.join(dic.get(x, x) for x in perm)


class WebHDFS(object):

    def __init__(self, namenode_host, namenode_port, hdfs_username, timeout=10):
        self.host = namenode_host
        self.port = namenode_port
        self.username = hdfs_username
        self.timeout = timeout

    def __pure(self, host, port, method, url, body=None, read=False, storeobj=None):

        def isNetworkError(e):
            return isinstance(e, socket.timeout) \
                or (hasattr(e, 'errno') and (e.errno == 10061 or e.errno == 61))

        httpClient = None
        try:
            data = None
            httpClient = HTTPConnection(host, port, timeout=self.timeout)
            httpClient.request(method, url, body, headers={})
            response = httpClient.getresponse()
            if response.status == 200 and read:
                if not storeobj:
                    data = response.read()
                else:
                    storeobj.begin()
                    while True:
                        buf = response.read(8192)
                        if not buf:
                            break
                        storeobj.write(buf)
                    storeobj.end()
            return response, data
        except Exception as e:
            if storeobj:
                storeobj.error(e)
            if not isNetworkError(e):
                raise e
            else:
                raise Exception("Network error, {0}".format(e))
        finally:
            if httpClient:
                httpClient.close()

    def __query(self, method, path, op, query=None, read=False):
        url = '/webhdfs/v1{0}?op={1}'.format(path, op)
        if self.username:
            url += '&user.name={0}'.format(self.username)
        if query:
            url += '&{0}'.format(urlencode(query))
        return self.__pure(self.host, self.port, method, url, read=read)

    def mkdir(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))
        response, _ = self.__query('PUT', path, 'MKDIRS')
        return response.status, response.reason

    def rmdir(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))
        response, _ = self.__query(
            'DELETE', path, 'DELETE', {'recursive': 'true'})
        return response.status, response.reason

    def put(self, data, target_file, replication=1):
        if os.path.isabs(target_file) == False:
            raise Exception(
                "Only absolute paths supported: %s" % (target_file)
            )
        response, _ = self.__query(
            'PUT', target_file, 'CREATE', {'overwrite': 'true'})
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
        response, _ = self.__query(
            'GET', target_file, 'OPEN', {'overwrite': 'true'})
        if response.length != None:
            result = urlparse(response.msg["location"])
            redirect_host = result.netloc[:result.netloc.index(":")]
            redirect_port = result.netloc[(result.netloc.index(":") + 1):]
            redirect_path = result.path + "?" + result.query
            response, data = self.__pure(
                redirect_host, redirect_port, 'GET', redirect_path, read=True, storeobj=storeobj)
        return response.status, response.reason, data

    def listdir(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))
        files = []
        response, data = self.__query('GET', path, 'LISTSTATUS', read=True)
        if data:
            data_dict = json.loads(py2or3str(data))
            if "FileStatuses" in data_dict:
                statuses = data_dict["FileStatuses"]
                for i in statuses["FileStatus"]:
                    files.append({
                        'name': i["pathSuffix"],
                        'owner': i["owner"],
                        'group': i["group"],
                        'replication': i["replication"],
                        'size': format_length(i["length"]),
                        'permission': format_permission(i["type"] == 'DIRECTORY', i["permission"])
                    })
        return response.status, response.reason, files

    def putFile(self, local_file, target_file, replication=1):
        with open(local_file, "rb") as file:
            file_obj = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                return self.put(file_obj, target_file, replication)
            finally:
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
