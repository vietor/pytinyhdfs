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
Usage: tinyhdfs [options] <command>

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -H HOST, --host=HOST  The server address for HDFS
  -p PORT, --port=PORT  The server port for HDFS, default: 50070
  -U USER, --user=USER  The username for HDFS, default: root

  ls <hdfs-path>:
    List information about directory

  get <hdfs-file> [local-path]:
    Download HDFS file to local

  put <file> <hdfs-path>:
    Upload file to HDFS

    -R REPLICATION, --replication=REPLICATION
                        The replication for upload, default: 2
    -G, --gzip          Try GZip compress before upload, file name append
                        ".gz"
    -D, --delete-source
                        Delete input file when upload success

```
