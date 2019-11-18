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
# description     : python SDCManager class
# author          : Yunkon(Alvin) Kim
# date            : 20190130
# version         : 0.1
# python_version  : 3.6
# notes           : This class is an implementation of a manager to handle SDC
#                   in the Python Programming Language.
# ==============================================================================

import logging
import os
# import logging
import random
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt import publish

import SoftwareDefinedCache as SDC

rootlogger = logging.getLogger(__name__)
rootlogger.setLevel("DEBUG")

SDC_id = "SDC_1"
client_id = "Client_1"

sdc = SDC.SoftwareDefinedCache(os.path.join(".", "cache"), 5)

MQTT_HOST = "163.180.117.236"
MQTT_PORT = 1883

# MQTT_HOST_ON_EDGE = "192.168.0.58"
# MQTT_PORT_ON_EDGE = 1883
MQTT_HOST_ON_EDGE = "163.180.117.185"
MQTT_PORT_ON_EDGE = 11883

# ----------------------------------------Error calculation for PID controller---------------------------------------#
# Assume 90% cache utilization
TARGET_UTILIZATION = 0.9

TEST_TIME = 30  # sec

is_testing = True

conditionLock = threading.Condition()

flow_control_delay = 0.03


def calculate_error(target, current):
    return target - current


# -------------------------------------------------------------------------------------------------------------------#

# ---------------------------------------------------- Producer ---------------------------------------------------- #
def notify_storage_status():
    # how long? many?
    global conditionLock
    try:
        start_time = time.time()
        with conditionLock:
            while True:
                # calculate error (utilization difference)
                # error = calculate_error(sdc.capacity * TARGET_UTILIZATION, sdc.used)
                # print("error :%s" % error)
                # error = calculate_error(sdc.capacity * TARGET_UTILIZATION, sdc.used)
                used = sdc.used
                print("<<=== feedback :%s" % used)
                # send feedback
                publish.single("core/edge/" + SDC_id + "/feedback", used, hostname=MQTT_HOST, port=MQTT_PORT, qos=2)
                conditionLock.wait()
                time.sleep(0.03)
                running_time = time.time() - start_time
                if running_time > TEST_TIME:
                    break
    except Exception as e:
        print("<<=== Exception: %s" % e)


# Creating threads

# # ----------------------------------------------------RESTful API----------------------------------------------------#
# app = Flask(__name__)
# api = Api(app)
#
#
# class Introduction:
#     def get(self):
#         introduction = """
#         Hello!
#         This is the RESTful API for Software-Defined Cache.
#         By "/help", you can see the list of Method(Create, Read, Update, Delete) and Resources(URI).
#         """
#         return introduction
#
#
# class Help(Resource):
#     def get(self):
#         help_message = """
#         API Usage:
#         - GET       /
#         - GET       /help
#         - GET       /api/data/
#         - GET       /api/data/<string:data_id>
#         """
#         return help_message
#
#
# class CachedData(Resource):
#     def get(self, data_id):
#         if not data_id:
#             print('First In First Out!')
#             data = sdc.read_first_data()
#         else:
#             print('The data out')
#             # read <data_id> and return
#
#         return data
#
#
# api.add_resource(Introduction, '/')
# api.add_resource(Help, '/help')
# api.add_resource(CachedData, '/api/data/', '/api/data/<string:data_id>')
#
#
# # -------------------------------------------------------------------------------------------------------------------#


# --------------------------------------------------MQTT Core-Edge----------------------------------------------------#
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("<<=== Connected to central MQTT broker - Result code: " + str(rc))
        client.subscribe("core/edge/" + SDC_id + "/data")
        client.subscribe("core/edge/" + SDC_id + "/flow_control")
        client.subscribe("core/edge/" + SDC_id + "/start_testing")

    else:
        print("<<=== Bad connection returned code = ", rc)
        print("<<=== ERROR: Could not connect to MQTT")


def on_message(client, userdata, msg):
    global conditionLock
    global flow_control_delay

    try:
        # print("Cart new message: " + msg.topic + " " + str(msg.payload))
        message = msg.payload
        print("<<=== Arrived topic: %s" % msg.topic)
        # print("Arrived message: %s" % message)

        if msg.topic == "core/edge/" + SDC_id + "/data":
            with conditionLock:
                # sdc.store_data(message)
                sdc.put_to_write_buffer(message)
                print("<<=== Cached data size: %s" % len(message))
                # data_size = int(message)
                # print("Data size: %s" % data_size)
                # sdc.used += data_size
                flow_control_delay = 0.03
                conditionLock.notify()
                # error = calculate_error(sdc.capacity * TARGET_UTILIZATION, sdc.used)
                # print("error: %s" % error)
                # time.sleep(0.03)
                # print("used: %s" % sdc.used)
                # publish.single("core/edge/" + SDC_id + "/feedback", sdc.used, hostname=MQTT_HOST, port=MQTT_PORT)
        elif msg.topic == "core/edge/" + SDC_id + "/flow_control":
            with conditionLock:
                print("<<=== ~~~~Rate_control~~~~")
                time.sleep(0.03)
                flow_control_delay += 0.03
                conditionLock.notify()
                # error = calculate_error(sdc.capacity * TARGET_UTILIZATION, sdc.used)
                # print("error: %s" % error)
                # print("used: %s" % sdc.used)
                # time.sleep(0.03)
                # publish.single("core/edge/" + SDC_id + "/feedback", sdc.used, hostname=MQTT_HOST, port=MQTT_PORT)
        elif msg.topic == "core/edge/" + SDC_id + "/start_testing":
            print("<<=== Start testing!!")
            storage_status_notifying = threading.Thread(target=notify_storage_status)
            storage_status_notifying.start()
            time.sleep(0.03)
            # publish.single("core/edge/" + SDC_id + "/feedback", sdc.used, hostname=MQTT_HOST, port=MQTT_PORT)
            publish.single("edge/client/" + client_id + "/start_caching", 1, hostname=MQTT_HOST_ON_EDGE,
                           port=MQTT_PORT_ON_EDGE, qos=2)
        else:
            print("<<=== Unknown - topic: " + msg.topic + ", message: " + message)
    except Exception as e:
        print("<<=== Exception: %s" % e)


def on_publish(client, userdata, mid):
    print("mid: " + str(mid))


def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(client, userdata, level, string):
    print(string)


# The below lines will be used to publish the topics
# publish.single("elevator/starting_floor_number", "3", hostname=MQTT_HOST, port=MQTT_PORT)
# publish.single("elevator/destination_floor_number", "2", hostname=MQTT_HOST, port=MQTT_PORT)
# ------------------------------------------------------------------------------------------------------------------#

# --------------------------------------------------MQTT Edge-Client---------------------------------------------------#
def on_local_connect(client, userdata, flags, rc):
    if rc == 0:
        print("===>> Connected to local MQTT broker - Result code: " + str(rc))
        client.subscribe("edge/client/" + client_id + "/data_req")
        client.subscribe("edge/client/" + client_id + "/done_to_test")

    else:
        print("===>> Bad connection returned code = ", rc)
        print("===>> ERROR: Could not connect to MQTT")


def on_local_message(client, userdata, msg):
    global is_testing

    try:
        # print("Cart new message: " + msg.topic + " " + str(msg.payload))
        message = msg.payload
        print("===>> Arrived topic: %s" % msg.topic)
        print("===>> Arrived message: %s" % message)

        if msg.topic == "edge/client/" + client_id + "/data_req":
            read_size = int(message)
            print("===>> Requested amount of data: %s" % read_size)
            data, result = sdc.read_bytes(read_size)
            #
            # if sdc.used < read_size:
            #     read_size = sdc.used
            #     sdc.used = 0
            # else:
            #     sdc.used -= read_size

            if result:
                print("===>> Length of transmitted data: %s" % len(data))
                publish.single("edge/client/" + client_id + "/data", data, hostname=MQTT_HOST_ON_EDGE,
                               port=MQTT_PORT_ON_EDGE, qos=2)
            else:
                print("===>> Cache misses (no data or not enough data)")
                publish.single("edge/client/" + client_id + "/data", False, hostname=MQTT_HOST_ON_EDGE,
                               port=MQTT_PORT_ON_EDGE, qos=2)

        elif msg.topic == "edge/client/" + client_id + "/done_to_test":
            publish.single("core/edge/" + SDC_id + "/done_to_test", "done", hostname=MQTT_HOST, port=MQTT_PORT, qos=2)
            time.sleep(3)
            is_testing = False
        else:
            print("===>> Unknown - topic: " + msg.topic + ", message: " + message)
    except Exception as e:
        print("Exception: %s" % e)


def on_local_publish(client, userdata, mid):
    print("mid: " + str(mid))


def on_local_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_local_log(client, userdata, level, string):
    print(string)


# The below lines will be used to publish the topics
# publish.single("elevator/starting_floor_number", "3", hostname=MQTT_HOST, port=MQTT_PORT)
# publish.single("elevator/destination_floor_number", "2", hostname=MQTT_HOST, port=MQTT_PORT)
# ------------------------------------------------------------------------------------------------------------------#


# ---------------------------------------------------- Consumer ---------------------------------------------------- #
# def consume_data_scenario1():
#     # A cloud service periodically consumes an equal amount of cached data.
#
#     start_time = time.time()
#     read_size = (2 << 19)
#     while True:
#         # consume data
#         # This section will be changed to apply the distributed messaging structure.
#         # In other words, MQTT will be used.
#         sdc.read_bytes(read_size)
#         # print("Consuming data")
#         running_time = time.time() - start_time
#         if running_time > TEST_TIME:
#             break
#         time.sleep(0.03)
#
#
# def consume_data_scenario2():
#     # A cloud service periodically consumes an unequal amount of cached data.
#
#     start_time = time.time()
#     # read_size = (4 << 19)
#     while True:
#         # This section will be changed to apply the distributed messaging structure.
#         val = random.randint(1, 2)
#         val2 = random.randint(16, 19)
#         read_size = (val << val2)
#         # In other words, MQTT will be used.
#         sdc.read_bytes(read_size)
#         # print("Consuming data")
#         running_time = time.time() - start_time
#         if running_time > TEST_TIME:
#             break
#         time.sleep(0.03)
#
#
# def consume_data_scenario3():
#     # A cloud service aperiodically consumes an equal amount of cached data.
#
#     start_time = time.time()
#     read_size = (2 << 19)
#     while True:
#         # This section will be changed to apply the distributed messaging structure.
#         # In other words, MQTT will be used.
#         random_ms = random.randint(3, 100) / 1000.0
#         sdc.read_bytes(read_size)
#         # print("Consuming data")
#         running_time = time.time() - start_time
#         if running_time > TEST_TIME:
#             break
#         time.sleep(random_ms)
#
#
# def consume_data_scenario4():
#     # A cloud service aperiodically consumes an unequal amount of cached data.
#
#     start_time = time.time()
#     while True:
#         # This section will be changed to apply the distributed messaging structure.
#         val = random.randint(1, 2)
#         val2 = random.randint(16, 19)
#         random_ms = random.randint(3, 100) / 1000.0
#         read_size = (val << val2)
#         # In other words, MQTT will be used.
#         sdc.read_bytes(read_size)
#         # print("Consuming data")
#         running_time = time.time() - start_time
#         if running_time > TEST_TIME:
#             break
#         time.sleep(random_ms)


def main():
    global is_testing
    # RESTful API runs
    # app.run(debug=True)

    # MQTT connection
    message_client = mqtt.Client("Edge1")
    message_client.on_connect = on_connect
    message_client.on_message = on_message
    # message_client.on_log = on_log
    # Connect to MQTT broker
    # message_client.connect(MQTT_HOST, MQTT_PORT, 60)
    # message_client.loop_start()

    # MQTT connection
    message_local_client = mqtt.Client("Edge2")
    message_local_client.on_connect = on_local_connect
    message_local_client.on_message = on_local_message
    # message_local_client.on_log = on_local_log

    # message_local_client.connect(MQTT_HOST_ON_EDGE, MQTT_PORT_ON_EDGE, 60)
    # message_local_client.loop_start()

    # Software-Defined Cache runs
    sdc.run()
    print("SDC runs")
    scenario_counter = 1

    while scenario_counter <= 4:

        # preparation time
        time.sleep(5)

        loop_counter = 0
        loop_round = 9

        while loop_counter < loop_round:
            # ---------- Scenario 1

            print("Truncate cache")
            directory = os.path.join(".", "cache")
            files = sorted(os.listdir(directory))
            for file_name in files:
                full_path = os.path.join(directory, file_name)
                os.remove(full_path)

            print("Start testing")
            time.sleep(3)

            sdc.initialize()

            message_client.connect(MQTT_HOST, MQTT_PORT, 60)
            message_client.loop_start()
            message_local_client.connect(MQTT_HOST_ON_EDGE, MQTT_PORT_ON_EDGE, 60)
            message_local_client.loop_start()

            publish.single("core/edge/" + SDC_id + "/init_for_testing", scenario_counter, hostname=MQTT_HOST,
                           port=MQTT_PORT, qos=2)

            # # Creating threads
            # t1 = threading.Thread(target=consume_data_scenario1)
            # # Starting threads
            # t1.start()
            # # Wait until threads are completely executed
            # t1.join()
            # print("Test 1 is done!")
            while is_testing:
                time.sleep(0.005)

            print("Done to test")
            message_client.loop_stop()
            message_client.disconnect()
            message_local_client.loop_stop()
            message_local_client.disconnect()

            is_testing = True

            # time.sleep(2)
            #
            # message_client.connect(MQTT_HOST, MQTT_PORT, 60)
            # message_client.loop_start()
            #
            # # ---------- Scenario 2
            #
            # print("Truncate cache")
            # directory = os.path.join(".", "cache")
            # files = sorted(os.listdir(directory))
            # for file_name in files:
            #     full_path = os.path.join(directory, file_name)
            #     os.remove(full_path)
            #
            # print("Start testing")
            # time.sleep(2)
            #
            # sdc.initialize()
            #
            # publish.single("core/edge/" + SDC_id + "/init_for_testing", 0, hostname=MQTT_HOST,
            #                port=MQTT_PORT)
            #
            # # Creating threads
            # t2 = threading.Thread(target=consume_data_scenario2)
            # # Starting threads
            # t2.start()
            # # Wait until threads are completely executed
            # t2.join()
            # print("Test 2 is done!")
            #
            # publish.single("core/edge/" + SDC_id + "/done_to_test", "done", hostname=MQTT_HOST, port=MQTT_PORT)
            # time.sleep(3)
            # message_client.loop_stop()
            # message_client.disconnect()
            # time.sleep(2)
            #
            # message_client.connect(MQTT_HOST, MQTT_PORT, 60)
            # message_client.loop_start()
            #
            # # ---------- Scenario 3
            #
            # print("Truncate cache")
            # directory = os.path.join(".", "cache")
            # files = sorted(os.listdir(directory))
            # for file_name in files:
            #     full_path = os.path.join(directory, file_name)
            #     os.remove(full_path)
            #
            # print("Start testing")
            # time.sleep(5)
            #
            # sdc.initialize()
            #
            # publish.single("core/edge/" + SDC_id + "/init_for_testing", 0, hostname=MQTT_HOST,
            #                port=MQTT_PORT)
            #
            # # Creating threads
            # t3 = threading.Thread(target=consume_data_scenario3)
            # # Starting threads
            # t3.start()
            # # Wait until threads are completely executed
            # t3.join()
            # print("Test 3 is done!")
            #
            # publish.single("core/edge/" + SDC_id + "/done_to_test", "done", hostname=MQTT_HOST, port=MQTT_PORT)
            # time.sleep(3)
            # message_client.loop_stop()
            # message_client.disconnect()
            # time.sleep(2)
            #
            # message_client.connect(MQTT_HOST, MQTT_PORT, 60)
            # message_client.loop_start()
            #
            # # ---------- Scenario 4
            #
            # sdc.initialize()
            #
            # print("Truncate cache")
            # directory = os.path.join(".", "cache")
            # files = sorted(os.listdir(directory))
            # for file_name in files:
            #     full_path = os.path.join(directory, file_name)
            #     os.remove(full_path)
            #
            # print("Start testing")
            # time.sleep(2)
            #
            # publish.single("core/edge/" + SDC_id + "/init_for_testing", 0, hostname=MQTT_HOST,
            #                port=MQTT_PORT)
            #
            # # Creating threads
            # t4 = threading.Thread(target=consume_data_scenario4)
            # # Starting threads
            # t4.start()
            # # Wait until threads are completely executed
            # t4.join()
            # print("Test 4 is done!")
            #
            # publish.single("core/edge/" + SDC_id + "/done_to_test", "done", hostname=MQTT_HOST, port=MQTT_PORT)
            # time.sleep(3)
            # message_client.loop_stop()
            # message_client.disconnect()
            # time.sleep(2)

            loop_counter += 1

        scenario_counter += 1

    print("last")
    message_client.connect(MQTT_HOST, MQTT_PORT, 60)
    message_client.loop_start()

    publish.single("core/edge/" + SDC_id + "/all_test_complete", "complete", hostname=MQTT_HOST, port=MQTT_PORT, qos=2)
    print("All threads is done!")
    print("Notify - All test complete")
    time.sleep(2)

    # ## communication test section - start
    # while True:
    #     time.sleep(1)
    #     publish.single("core/edge/" + SDC_id + "/error", "5", hostname=MQTT_HOST, port=MQTT_PORT)

    # ## communication test section - end

    # publish.single("core/edge/" + SDC_id + "/error", "5", hostname=MQTT_HOST, port=MQTT_PORT)

    # development plan
    # 1. initialize cache (path, cache capacity, data queue)
    # 2. recv data
    # 3. calculate error
    # 4. send error
    # 5. RESTful API in this cache

    message_client.loop_stop()
    message_local_client.loop_stop()
    # Threads completely executed


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print("Exception: %s" % e)
