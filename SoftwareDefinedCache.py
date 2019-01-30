# This file is part of Qualified Caching-as-a-Service.
# Copyright 2019 Intelligent-distributed Cloud and Security Laboratory (ICNS Lab.)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
# associated documentation files (the "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
#
# title           : SoftwareDefinedCache.py
# description     : python SoftwareDefinedCache class
# author          : Yunkon(Alvin) Kim
# date            : 20190130
# version         : 0.1
# python_version  : 3.6
# notes           : This SoftwareDefinedCache is an implementation of a cache managed by software
#                   in the Python Programming Language.
# ==============================================================================
import os
import threading
import time
import json


class SoftwareDefinedCache:

    def __init__(self, directory, capacity, data_retention_period=900):
        """initialize this class

        :param directory: the dir to create files for cache
        :param capacity: the capacity of the cache (unit: MB)
        :param data_retention_period: the data retention period to delete data (unit: second)
        """
        self.directory = directory
        self.capacity = capacity
        self.used = 0
        self.data_retention_period = data_retention_period
        self.__lock = threading.Lock()

    # (Not covered here) recv data <- SDC Manager is in charge of communication with EDCrammer or a cloud service.
    # store data to cache
    def store_data(self, data):
        file_name = time.strftime("%Y%m%d%H%M%S")
        full_path = os.path.join(self.directory, file_name)

        f = open(full_path, 'w')
        f.write(data)

        # read file size and convert byte to megabyte
        self.used += (os.path.getsize(full_path) >> 20)

        f.close()

    # !!! the other function to read data is necessary. (e.g., "index.html" file it self) it depends on a service.
    # read first data from a directory
    def read_first_data(self):
        self.__lock.acquire()

        files = sorted(os.listdir(self.directory))
        file_name = files.pop(0)
        full_path = os.path.join(self.directory, file_name)

        f = open(full_path, 'r')
        data = f.read()
        f.close()

        self.__lock.release()

        return data

    def decay_data(self):
        """Running as a thread, this function deletes data considering last data hit.
        (e.g., certain period of time has passed since the last hit.)
        """
        # aging checking function will be added.
        current_time = time.time()

        self.__lock.acquire()

        files = sorted(os.listdir(self.directory))
        for file_name in files:
            full_path = os.path.join(self.directory, file_name)
            no_hit_period = current_time - os.stat(full_path).st_atime
            if no_hit_period > self.data_retention_period:
                os.remove(full_path)

        self.__lock.release()

    def run(self):
        t1 = threading.Thread(target=self.decay_data)
        t1.start()
        t1.join()

    # for monitoring cache status
    def get_cache_status(self):
        cache_status = {
            "directory": self.directory,
            "capacity": self.capacity,
            "used": self.used,
            "available": self.capacity-self.used,
            "data_retention_period": self.data_retention_period
        }
        return json.dumps(cache_status)
