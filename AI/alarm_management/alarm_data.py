import datetime
import time
import string
import urllib2
import math
import redis

import json
import py_cf
import os

import pika
import uuid
import json
import base64

from Rabbit_RPC_Base import *
from mangodb_queue   import *



class Analyize_Alarms(Rabbit_RPC_Base):

   def __init__(self, remote_list = remote_list, db=db, collection = collection, queue_depth = queue_depth ):
       self.remote_list = remote_list
       self.collection  = collection
       self.queue_depth = queue_depth
       self.client      = MongoClient()
       self.mongodb_queue = Mongodb_Queue( self.client,self.db,self.collection, self.queue_depth )
       self.db          = db


   def download_alarms( self, *args ):
       return_value = {}
       for i in remote_list.keys():
          remote_data      = remote_list[i]
          vhost            = remote_data["vhost"]
          rpc_queue        = remote_data["rpc_queue"]
          time_out         = remote_data["time_out"]
          remote_queue     = remote_data["remote_queue"]
          down_load_number = remote_data["down_load_number"]
          return_value[vhost] = self.pop_queue( vhost=None,rpc_queue=rpc_queue, time_out=time_out, remote_queue=remote_queue, down_load_number= down_load_number)
       return return_value


   def process_alarms( self,*args):
       alarm_list = self.download_alarms()       
       for vhost in alarm_list.keys():
           alarm_data = alarm_list[ vhost ]
           self.mongodb_queue.push( vhost, json.dumps( alarm_data ) )

