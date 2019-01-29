import logging
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt import publish

serLock = threading.Lock()

mylogger = logging.getLogger("my")

worker_id = "worker_1"


# --------------------------------------------------------MQTT----------------------------------------------------------#
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected - Result code: " + str(rc))
        client.subscribe("core/edge/" + worker_id + "/data")

    else:
        print("Bad connection returned code = ", rc)


def on_message(client, userdata, msg):
    # print("Cart new message: " + msg.topic + " " + str(msg.payload))
    message = str(msg.payload)
    if msg.topic == "core/edge/" + worker_id + "/data":
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
# ---------------------------------------------------------------------------------------------------------------------#

if __name__ == '__main__':
    message_client = mqtt.Client(worker_id)
    message_client.on_connect = on_connect
    message_client.on_message = on_message

    # Connect to MQTT broker
    try:
        message_client.connect("163.180.117.37", 1883, 60)
    except:
        print("ERROR: Could not connect to MQTT")

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
        publish.single("core/edge/" + worker_id + "/error", "5", hostname="163.180.117.37", port=1883)
    # ## communication test section - end

    # development plan
    # 1. initialize cache (path, cache capacity, data queue)
    # 2. recv data
    # 3. calculate error
    # 4. send error

    message_client.loop_stop()
    # Threads completely executed
    print("All threads is done!")
