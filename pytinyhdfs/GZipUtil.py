# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import gzip


class GZipUtil(object):

    @staticmethod
    def compress(source_file, delete=False):
        target_file = source_file + ".gz"
        successed = False
        target_created = False
        with open(source_file, "rb") as file_in:
            with open(target_file, "wb") as file_out:
                target_created = True
                gzfile = gzip.GzipFile('', mode='wb', fileobj=file_out)
                try:
                    for line in file_in:
                        gzfile.write(line)
                        successed = True
                finally:
                    gzfile.close()

        if successed:
            if delete:
                os.remove(source_file)

        else:
            if target_created:
                os.remove(target_file)
                target_file = None

        return successed, target_file
