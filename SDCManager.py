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
# description     : python SDCManager class
# author          : Yunkon(Alvin) Kim
# date            : 20190130
# version         : 0.1
# python_version  : 3.6
# notes           : This class is an implementation of a manager to handle SDC
#                   in the Python Programming Language.
# ==============================================================================
from flask import Flask
from flask_restful import Resource, Api
# import logging
import threading
import time
import paho.mqtt.client as mqtt
from paho.mqtt import publish

serLock = threading.Lock()

# mylogger = logging.getLogger("my")

SDC_id = "SDC_1"

# ----------------------------------------------------RESTful API----------------------------------------------------#
app = Flask(__name__)
api = Api(app)


class Introduction:
    def get(self):
        introduction = """
        Hello!
        This is the RESTful API for Software-Defined Cache.
        By "/help", you can see the list of Method(Create, Read, Update, Delete) and Resources(URI). 
        """
        return introduction


class Help(Resource):
    def get(self):
        help_message = """
        API Usage:
        - GET       /
        - GET       /help
        - GET       /api/data/
        - GET       /api/data/<string:data_id>
        """
        return help_message


class CachedData(Resource):
    def get(self, data_id):
        if not data_id:
            print('First In First Out!')
            # read first and return
        else:
            print('The data out')
            # read <data_id> and return

        return {'task': 'Hello world'}


api.add_resource(Introduction, '/')
api.add_resource(Help, '/help')
api.add_resource(CachedData, '/api/data/', '/api/data/<string:data_id>')


# -------------------------------------------------------------------------------------------------------------------#


# -------------------------------------------------------MQTT--------------------------------------------------------#
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected - Result code: " + str(rc))
        client.subscribe("core/edge/" + SDC_id + "/data")

    else:
        print("Bad connection returned code = ", rc)
        print("ERROR: Could not connect to MQTT")


def on_message(client, userdata, msg):
    # print("Cart new message: " + msg.topic + " " + str(msg.payload))
    message = str(msg.payload)
    if msg.topic == "core/edge/" + SDC_id + "/data":
        print("Arrived message: " + message)
    else:
        print("Unknown - topic: " + msg.topic + ", message: " + message)


def on_publish(client, userdata, mid):
    print("mid: " + str(mid))


def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(client, userdata, level, string):
    print(string)


# The below lines will be used to publish the topics
# publish.single("elevator/starting_floor_number", "3", hostname="163.180.117.195", port=1883)
# publish.single("elevator/destination_floor_number", "2", hostname="163.180.117.195", port=1883)
# ------------------------------------------------------------------------------------------------------------------#

if __name__ == '__main__':
    # RESTful API runs
    app.run(debug=True)

    # MQTT connection
    message_client = mqtt.Client(SDC_id)
    message_client.on_connect = on_connect
    message_client.on_message = on_message

    # Connect to MQTT broker

    message_client.connect("163.180.117.37", 1883, 60)

    print("MQTT client start")
    message_client.loop_start()

    # lock = threading.Lock()
    # Creating thread
    # t1 = threading.Thread(target=func, args=[xxx])
    # Starting thread 1
    # t1.start()
    # Wait until thread 1 is completely executed
    # t1.join()

    # ## communication test section - start
    while True:
        time.sleep(1)
        publish.single("core/edge/" + SDC_id + "/error", "5", hostname="163.180.117.37", port=1883)
    # ## communication test section - end

    # development plan
    # 1. initialize cache (path, cache capacity, data queue)
    # 2. recv data
    # 3. calculate error
    # 4. send error
    # 5. RESTful API in this cache

    message_client.loop_stop()
    # Threads completely executed
    print("All threads is done!")
