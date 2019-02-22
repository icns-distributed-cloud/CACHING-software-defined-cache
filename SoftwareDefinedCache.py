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
import sys
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
        self._lock = threading.Lock()
        self._used_lock = threading.Lock()
        self.directory = directory
        self.capacity = (capacity << 20)
        self._used = 0
        self.data_retention_period = data_retention_period
        self.seek_start_point = 0

    # (Not covered here) recv data <- SDC Manager is in charge of communication with EDCrammer or a cloud service.
    # store data to cache
    def store_data(self, data):
        file_name = time.strftime("%Y%m%d%H%M%S")
        full_path = os.path.join(self.directory, file_name)

        f = open(full_path, 'wb')
        f.write(data)

        # read file size and convert byte to megabyte
        self.used += os.path.getsize(full_path)

        f.close()

    # !!! the other function to read data is necessary. (e.g., "index.html" file it self) it depends on a service.
    # read first data from a directory
    def read_first_data(self):
        data = False
        if self.used > 0:
            self._lock.acquire()

            files = sorted(os.listdir(self.directory))
            if len(files) > 0:
                file_name = files.pop(0)
                full_path = os.path.join(self.directory, file_name)

                f = open(full_path, 'rb')

                data = f.read()
                f.close()

                self.used -= os.path.getsize(full_path)
                os.remove(full_path)

            self._lock.release()

        return data

    def read_bytes(self, size):
        data = False
        if self.used > 0:
            self._lock.acquire()

            files = sorted(os.listdir(self.directory))
            if len(files) > 0:
                file_name = files.pop(0)
                full_path = os.path.join(self.directory, file_name)

                f = open(full_path, 'rb')

                f.seek(self.seek_start_point)

                data = f.read(size)
                f.close()

                data_size = len(data)
                file_size = os.path.getsize(full_path)
                self.used -= data_size

                # update seek_start_point for future access
                self.seek_start_point += data_size

                # print("Size: %s /// data_size: %s" %
                #       (size, data_size))

                print("Used: %s /// seek_start_point: %s /// file_size: %s" %
                      (self.used, self.seek_start_point, file_size))

                if file_size == self.seek_start_point:
                    print("!!!!!!!!!!!!!!!!DELETE %s" % full_path)
                    os.remove(full_path)
                    self.seek_start_point = 0

            self._lock.release()

        return data

    def decay_data(self):
        """Running as a thread, this function deletes data considering last data hit.
        (e.g., certain period of time has passed since the last hit.)
        """
        # aging checking function will be added.
        current_time = time.time()

        self._lock.acquire()

        files = sorted(os.listdir(self.directory))
        for file_name in files:
            full_path = os.path.join(self.directory, file_name)
            no_hit_period = current_time - os.stat(full_path).st_atime
            if no_hit_period > self.data_retention_period:
                os.remove(full_path)

        self._lock.release()

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

    @property
    def used(self):
        return self._used

    @used.setter
    def used(self, val):
        self._used_lock.acquire()
        self._used = val
        self._used_lock.release()
