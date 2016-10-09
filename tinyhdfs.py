#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, with_statement

import re
import os
import sys
from optparse import *

from pytinyhdfs import WebHDFS
from pytinyhdfs import GZipUtil

VERSION = "1.1.4"

TYPE_FILE = 1
TYPE_DIRECTORY = 2
TYPE_MAYBE_DIRECTORY = 3


def _check_type(webhdfs, target_path, ftype):
    status, reason, fstatus = webhdfs.status(target_path)
    if status != 200:
        if status != 404 or ftype != TYPE_MAYBE_DIRECTORY:
            raise Exception("Status failed: [%d, %s]" % (status, reason))
    elif ftype == TYPE_FILE:
        if fstatus['type'] != 'FILE':
            raise Exception('Status failed: target type not FILE')
    elif ftype == TYPE_DIRECTORY or ftype == TYPE_MAYBE_DIRECTORY:
        if fstatus['type'] != 'DIRECTORY':
            raise Exception('Status failed: target type not DIRECTORY')


def _format_size(size):
    def ffloat(v):
        return '{0:0.2f}'.format(v).rstrip('0').rstrip('.')

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


def _command_ls(webhdfs, target_path):
    status, reason, files = webhdfs.listdir(target_path)
    if status != 200:
        raise Exception("List failed: [%d, %s]" % (status, reason))
    print('Found {0} items'.format(len(files)))
    for row in files:
        print('{0:10s} {1:12s} {2:12s} {3:12s} {4}'.format(
            row['permission'],
            row['owner'],
            row['group'],
            _format_size(row['size']),
            row['name']
        ))


def command_ls(webhdfs, target_path):
    try:
        _check_type(webhdfs, target_path, TYPE_DIRECTORY)
        _command_ls(webhdfs, target_path)
    except Exception as e:
        print(e)


def _command_get(webhdfs, target_file, local_path):
    _, filename = os.path.split(target_file)
    local_file = os.path.join(local_path, filename)
    status, reason = webhdfs.getFile(target_file, local_file)
    if status != 200:
        raise Exception("Get failed: [%d, %s]" % (status, reason))


def command_get(webhdfs, target_file, local_path):
    try:
        _check_type(webhdfs, target_file, TYPE_FILE)
        _command_get(webhdfs, target_file, local_path)
    except Exception as e:
        print(e)


def _command_put(webhdfs, source_file, target_path, options):
    compressed_file = None
    source_filename = os.path.split(source_file)[1]
    try:
        upload_file = source_file
        target_file = target_path.rstrip("/") + "/" + source_filename
        if options.gzip and not upload_file.endswith('.gz'):
            successed, compressed_file = GZipUtil.compress(upload_file)
            if successed:
                upload_file = compressed_file
                target_file = target_file + ".gz"
        status, reason = webhdfs.putFile(upload_file, target_file,
                                         replication=options.replication,
                                         overwrite=options.overwrite)
        if status >= 200 and status < 400:
            if options.delete_source:
                os.remove(source_file)
        else:
            raise Exception("Put failed: [%d, %s]" % (status, reason))
        print("File: <%s>, Successed" % (source_file))
    except Exception as e:
        raise Exception("File: <%s>, Exception: %s" %
                        (source_file, "{0}".format(e)))
    finally:
        if compressed_file:
            os.remove(compressed_file)
            compressed_file = None


def command_put(webhdfs, source_file, target_path, options):
    workdir, filename = os.path.split(source_file)
    if "*" not in filename:
        if not os.path.exists(source_file):
            print("File not exists: " + source_file)
        else:
            try:
                _check_type(webhdfs, target_path, TYPE_MAYBE_DIRECTORY)
                _command_put(webhdfs, source_file, target_path, options)
            except Exception as e:
                print(e)
    else:
        if len(workdir) < 1:
            workdir = os.getcwd()
        if not os.path.exists(workdir):
            print("Path not exists: " + workdir)
        else:
            try:
                _check_type(webhdfs, target_path, TYPE_MAYBE_DIRECTORY)
                pattern = re.compile(filename
                                     .replace(".", "\\.")
                                     .replace("*", ".*"))
                filenames = filter(lambda x: pattern.match(x) and os.path.isfile(os.path.join(workdir, x)),
                                   os.listdir(workdir))
                for name in filenames:
                    try:
                        _command_put(webhdfs, os.path.join(workdir, name),
                                     target_path, options)
                    except Exception as e:
                        print(e)
            except Exception as e:
                print(e)


def _command_rm(webhdfs, target_file):
    status, reason = webhdfs.delete(target_file)
    if status != 200:
        raise Exception("Delete failed: [%d, %s]" % (status, reason))


def command_rm(webhdfs, target_file):
    try:
        _check_type(webhdfs, target_file, TYPE_FILE)
        _command_rm(webhdfs, target_file)
    except Exception as e:
        print(e)


def _command_rmdir(webhdfs, target_path, options):
    status, reason = webhdfs.delete(target_path, options.recursive)
    if status != 200:
        raise Exception("Delete failed: [%d, %s]" % (status, reason))


def command_rmdir(webhdfs, target_path, options):
    try:
        _check_type(webhdfs, target_path, TYPE_DIRECTORY)
        _command_rmdir(webhdfs, target_path, options)
    except Exception as e:
        print(e)


def _command_mkdir(webhdfs, target_path):
    status, reason = webhdfs.mkdir(target_path)
    if status != 200:
        raise Exception("Create failed: [%d, %s]" % (status, reason))


def command_mkdir(webhdfs, target_path):
    try:
        _command_mkdir(webhdfs, target_path)
    except Exception as e:
        print(e)


def main():

    def die(message=None):
        if message and len(message) > 0:
            print(message)
        sys.exit(0)

    def enforce_args(args, size):
        if len(args) != size:
            die("Command <%s>: Invalid paramters!, use --help for more details" %
                args[0])
        return len(args)

    def enforce_args2(args, size1, size2):
        if len(args) != size1 and len(args) != size2:
            die("Command <%s>: Invalid paramters!, use --help for more details" %
                args[0])
        return len(args)

    def parse_hdfs_path(path):
        if not path.startswith("hdfs:///"):
            if path.startswith("/"):
                return path
            else:
                die("Parameter: HDFS URI must start with \"hdfs:///\" or \"/\"")
        return path[7:]

    def parse_env_host():
        return os.getenv("TINYHDFS_HOST")

    def parse_env_port():
        return int(os.getenv("TINYHDFS_PORT") or "50070")

    def parse_username():
        return os.getenv("USER") or os.getenv("USERNAME") or "root"

    parser = OptionParser("%prog [options] <command>", version=VERSION,
                          description="Tiny client for HDFS, base on WebHDFS")
    parser.add_option("-H", "--host",
                      dest="host",
                      default=parse_env_host(),
                      help="The server address for HDFS, default: env[\"TINYHDFS_HOST\"]")
    parser.add_option("-p", "--port",
                      type="int", dest="port",
                      default=parse_env_port(),
                      help="The server port for HDFS, default: env[\"TINYHDFS_PORT\"] or 50070")
    parser.add_option("-T", "--timeout",
                      type="int", dest="timeout",
                      default=10,
                      help="The timeout for HDFS connection, default: 10 (seconds)")
    parser.add_option("-U", "--user",
                      dest="user",
                      default=parse_username(),
                      help="The username connect for HDFS")

    group = OptionGroup(parser, "ls <hdfs-path>",
                        "List information about directory")
    parser.add_option_group(group)

    group = OptionGroup(parser, "rm <hdfs-file>",
                        "Remove file from HDFS")
    parser.add_option_group(group)

    group = OptionGroup(parser, "rmdir <hdfs-path>",
                        "Remove directory from HDFS")
    group.add_option("-r", "--recursive",
                     action="store_true", dest="recursive",
                     default=False,
                     help="Recursive delete child directory")
    parser.add_option_group(group)

    group = OptionGroup(parser, "mkdir <hdfs-path>",
                        "Create directory in HDFS")
    parser.add_option_group(group)

    group = OptionGroup(parser, "get <hdfs-file> [local-path]",
                        "Download HDFS file to local, default $PWD")
    parser.add_option_group(group)

    group = OptionGroup(parser, "put <local-file> <hdfs-path>",
                        "Upload file to HDFS, support \"*\" for multiple files")
    group.add_option("-N", "--no-overwrite",
                     action="store_false", dest="overwrite",
                     default=True,
                     help="Disable overwrite exists file for upload")
    group.add_option("-R", "--replication",
                     type="int", dest="replication",
                     default=2,
                     help="The replication for upload, default: 2")
    group.add_option("-G", "--gzip",
                     action="store_true", dest="gzip",
                     default=False,
                     help="Try GZip compress before upload, file name append \".gz\"")
    group.add_option("-D", "--delete-source",
                     action="store_true", dest="delete_source",
                     default=False,
                     help="Delete input file when upload success")
    parser.add_option_group(group)

    if len(sys.argv) > 1:
        sys_argv = sys.argv
    else:
        sys_argv = [sys.argv[0], "--help"]

    (options, args) = parser.parse_args(sys_argv)
    if not options.host:
        die("lost options: -H or --host or env[\"TINYHDFS_HOST\"]")

    args = args[1:]
    webhdfs = WebHDFS(options.host, options.port, options.user,
                      timeout=options.timeout)

    if len(args) < 1:
        parser.print_help()
        die()

    if args[0] == "ls":
        enforce_args(args, 2)
        command_ls(webhdfs, parse_hdfs_path(args[1]))

    elif args[0] == "get":
        if enforce_args2(args, 3, 2) == 3:
            command_get(webhdfs, parse_hdfs_path(args[1]), args[2])
        else:
            command_get(webhdfs, parse_hdfs_path(args[1]), os.getcwd())

    elif args[0] == "put":
        enforce_args(args, 3)
        command_put(webhdfs, args[1], parse_hdfs_path(args[2]), options)

    elif args[0] == "rm":
        enforce_args(args, 2)
        command_rm(webhdfs, parse_hdfs_path(args[1]))

    elif args[0] == "rmdir":
        enforce_args(args, 2)
        command_rmdir(webhdfs, parse_hdfs_path(args[1]), options)

    elif args[0] == "mkdir":
        enforce_args(args, 2)
        command_mkdir(webhdfs, parse_hdfs_path(args[1]))

    else:
        die("Not found supported command!, use --help for more details")

if __name__ == '__main__':
    main()
