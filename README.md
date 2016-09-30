pytinyhdfs
========

[![PYPI release](https://img.shields.io/pypi/v/tinyhdfs.svg)](https://pypi.python.org/pypi/tinyhdfs)
[![License MIT](https://img.shields.io/github/license/vietor/pytinyhdfs.svg)](http://opensource.org/licenses/MIT)

Tiny client for HDFS, base on WebHDFS

# Install

``` bash
pip install tinyhdfs
```

# Usage

``` bash
tinyhdfs --help
```

## ls \<hdfs-path\>

List information about directory

## rm \<hdfs-file\>

Remove file from HDFS

## rmdir \<hdfs-path\>

Remove directory from HDFS

##  mkdir \<hdfs-path\>

Create directory in HDFS

## get \<hdfs-file\> [local-path]

Download HDFS file to local, default $PWD

## put \<local-file\> \<hdfs-path\>

Upload file to HDFS, support "*" for multiple files
