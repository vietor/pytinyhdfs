pytinyhdfs
========

[![PYPI release](https://img.shields.io/pypi/v/tinyhdfs.svg)](https://pypi.python.org/pypi/tinyhdfs)
[![License MIT](https://img.shields.io/github/license/vietor/pytinyhdfs.svg)](http://opensource.org/licenses/MIT)

Tiny client for HDFS, base on WebHDFS

# Install

## pip

``` sh
pip install tinyhdfs
```

# Usage

``` sh
tinyhdfs --help
Usage: tinyhdfs [options] <command>

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -H HOST, --host=HOST  The server address for HDFS, default:
                        env["TINYHDFS_HOST"]
  -p PORT, --port=PORT  The server port for HDFS, default: 50070
  -T TIMEOUT, --timeout=TIMEOUT
                        The timeout for HDFS connection, default: 10 (seconds)
  -U USER, --user=USER  The username connect for HDFS

  ls <hdfs-path>:
    List information about directory

  rm <hdfs-file>:
    Remove file from HDFS

  rmdir <hdfs-path>:
    Remove directory from HDFS

    -r, --recursive     Recursive delete child directory

  mkdir <hdfs-path>:
    Create directory in HDFS

  get <hdfs-file> [local-path]:
    Download HDFS file to local

  put <local-file> <hdfs-path>:
    Upload file to HDFS, support "*" for multiple files

    -N, --no-overwrite  Disable overwrite exists file for upload
    -R REPLICATION, --replication=REPLICATION
                        The replication for upload, default: 2
    -G, --gzip          Try GZip compress before upload, file name append
                        ".gz"
    -D, --delete-source
                        Delete input file when upload success
```

