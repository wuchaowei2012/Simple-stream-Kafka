import cv2 as cv
import numpy as np

import requests
import json
import os.path as osp
import os
import sys

import pandas as pd
from multiprocessing.pool import Pool


import pulsar
client = pulsar.Client('pulsar://pulsar.rc.com:6650')
consumer = client.subscribe('my-topic', 'my-subscription')


while True:
    msg = consumer.receive()
    try:
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)



for total in range(5):
    
    msg = consumer.receive()
    try:
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)




client.close()