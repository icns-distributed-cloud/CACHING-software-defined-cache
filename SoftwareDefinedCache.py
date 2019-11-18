# This file is part of Qualified Caching-as-a-Service.
# BSD 3-Clause License
#
# Copyright (c) 2019, Intelligent-distributed Cloud and Security Laboratory (ICNS Lab.)
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
from datetime import datetime
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
        self._buffer_lock = threading.Lock()
        self._is_not_full_cache = threading.Condition()
        self._is_not_empty_buffer = threading.Condition()
        self.directory = directory
        self.capacity = (capacity << 20)
        self._used = 0
        self.data_retention_period = data_retention_period
        self.seek_start_point = 0
        self._buffer = bytearray()

    def initialize(self):
        self._used = 0
        self.seek_start_point = 0

    def put_to_write_buffer(self, data):
        with self._is_not_empty_buffer:
            # print("Type of data: %s" % type(data))
            self._buffer_lock.acquire()
            self.buffer.extend(data)
            len_of_buffer = len(self.buffer)
            print("===== Buffer len: %s" % format(len_of_buffer, ","))
            self._buffer_lock.release()
            if len_of_buffer > 0:
                self._is_not_empty_buffer.notify()

    def store_data_with_write_buffer(self):
        print("===== Thread start - store data with write-buffer")
        with self._is_not_empty_buffer:
            while True:
                self._is_not_empty_buffer.wait()

                with self._is_not_full_cache:

                    self._buffer_lock.acquire()
                    len_of_buffer = len(self.buffer)
                    self._buffer_lock.release()

                    if len_of_buffer > 0:
                        remaining_capacity = self.capacity - self.used
                        if remaining_capacity > 0:
                            self._buffer_lock.acquire()
                            data = self.buffer[:remaining_capacity]
                            self.buffer = self.buffer[remaining_capacity:]
                            self._buffer_lock.release()
                            self.store_data(data)
                            print("===== Stored data length: %s" % format(len(data), ","))
                        elif remaining_capacity == 0:   # Cache is full
                            print("===== CACHE IS FULL. Waiting until not full")
                            self._is_not_full_cache.wait()
                        else:
                            print("===== Unknown process (remaining_capacity < 0)")
                    elif len_of_buffer == 0:
                        print("===== Buffer is empty! (%s)" % len_of_buffer)
                    else:
                        print("===== Unknown (Buffer is lower than 0)")

    # (Not covered here) recv data <- SDC Manager is in charge of communication with EDCrammer or a cloud service.
    # store data to cache
    def store_data(self, data):
        # file name should includes milliseconds because caching is occurred quickly.
        # [:-3] means convert microseconds to milliseconds
        self._lock.acquire()
        file_name = datetime.now().strftime("%Y%m%d%H%M%S.%f")[:-3]  # time.strftime("%Y%m%d%H%M%S")
        full_path = os.path.join(self.directory, file_name)

        f = open(full_path, 'wb')
        f.write(data)

        # read file size and convert byte to megabyte
        self.used += os.path.getsize(full_path)

        f.close()
        self._lock.release()

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
        data = b''
        result = False
        print("===== Utilization: %s" % self.used)
        try:

            if self.used >= size:
                with self._is_not_full_cache:
                    self._lock.acquire()
                    # if used is bigger or equal with size, the below 2 lines are naturally passed.
                    # files = sorted(os.listdir(self.directory))
                    # if len(files) > 0:
                    data_size = 0

                    while data_size < size:
                        files = sorted(os.listdir(self.directory))
                        # always pop 0 because the read file is deleted when seek_pointer reaches EOF.
                        file_name = files.pop(0)
                        full_path = os.path.join(self.directory, file_name)

                        f = open(full_path, 'rb')

                        f.seek(self.seek_start_point)
                        read_data = f.read(size - data_size)

                        f.close()

                        read_data_size = len(read_data)
                        file_size = os.path.getsize(full_path)
                        self.used -= read_data_size

                        # update seek_start_point for future access
                        self.seek_start_point += read_data_size

                        # print("Size: %s /// data_size: %s" %
                        #       (size, data_size))

                        # print("Used: %s /// seek_start_point: %s /// file_size: %s" %
                        #       (self.used, self.seek_start_point, file_size))

                        if file_size <= self.seek_start_point:
                            # print("!!!!!!!!!!!!!!!!DELETE %s" % full_path)
                            os.remove(full_path)
                            self.seek_start_point = 0

                        data += read_data
                        data_size += read_data_size

                    result = True
                    self._lock.release()
                    self._is_not_full_cache.notify()
        except Exception as e:
            result = False
            print("!!!!! Exception: %s" % e)

        return data, result

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
        # t1 = threading.Thread(target=self.decay_data)
        t1 = threading.Thread(target=self.store_data_with_write_buffer)
        t1.start()
        # t1.join()

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
        self._used_lock.acquire()
        val = self._used
        self._used_lock.release()
        return val

    @used.setter
    def used(self, val):
        self._used_lock.acquire()
        self._used = val
        self._used_lock.release()

    @property
    def buffer(self):
        return self._buffer

    @buffer.setter
    def buffer(self, buffer):
        self._buffer = buffer
