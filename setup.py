#!/usr/bin/env python

from __future__ import print_function, with_statement

import re
import sys
from setuptools import setup, find_packages

SCRIPT_URI = 'bin/tinyhdfs'
GITHUB_URL = 'https://github.com/vietor/pytinyhdfs'

VERSION = 'UNKNOW'
try:
    with open(SCRIPT_URI, 'r') as f:
        m = re.search('VERSION[\ ]*\=[\ ]*\"([0-9\.]*)\"', f.read())
        if not m:
            raise Exception("Not found `VERSION` in script file")
        VERSION = m.group(1)
except IOError as e:
    raise Exception("Not found the script file: " + SCRIPT_URI)

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    os.system('python setup.py bdist_wheel upload')
    sys.exit()

setup(
    name='pytinyhdfs',
    version=VERSION,
    keywords='webhdfs, hdfs',
    description='Another WebHDFS tools',
    author='Vietor Liu',
    author_email='vietor.liu@gmail.com',
    url=GITHUB_URL,
    download_url='{0}/tarball/{1}'.format(GITHUB_URL, VERSION),
    license='MIT',
    scripts=['bin/tinyhdfs'],
    requires=[
        'python (>=2.6.0)',
    ],
    classifiers=[
        'Programming Language :: Python',
        'Development Status :: 5 - Production/Stable',
        'Natural Language :: English',
        'Environment :: Console',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License'
    ]
)
