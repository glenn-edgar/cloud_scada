#
# File: ai_manager.py
#
#
#
#
#
import pika
import uuid
import json
import base64
import datetime
import time
import string
import urllib2
import math
import redis
import json
import py_cf 
import os
import math
import base64
from media_drivers.trello_management      import Trello_Management 
from neo4j_graph.graph_functions          import Query_Configuration
from rabbit.rabbitmq_client               import RabbitMq_Client
from rabbit.client_commands               import Status_Alert_Cmds        
from configure_rabbit_queues              import Rabbitmq_Remote_Connections
from mongodb.collection_functions         import Mongodb_Collection
from mongodb.collection_functions         import Mongodb_DataBase
from pymongo                              import MongoClient
from mongodb_capped_collections           import Capped_Collections
from update_irrigation_data               import Update_Irrigation_Data
from update_irrigation_valve_current_draw import Update_Irrigation_Valve_Current_Draw
from update_irrigation_valve_current_draw import Analyize_Valve_Current_Data
from remote_statistics                    import Analyize_Controller_Parameters
from remote_statistics                    import Analyize_Remote_Connectivity
from populate_trello_data_base            import Transfer_Data
import py_cf 


class Monitor_Event_Queues():

   def __init__( self, rabbitmq_remote_connections, query_configuration, chain_flow ):
       self.rc               = rabbitmq_remote_connections
       self.qc               = query_configuration
       self.cf               = chain_flow

   def process_card( self, card_dict, event_data ):
       card_node = card_dict["card_node"]
       label     = card_dict["card_action"]["label"]
       #print "event_data",event_data["event"]
       #print "card action",card_dict["card_action"].keys()
       if label ==  "fromevent":
         label = event_data["status"].lower()
         #print "---------->",label,event_data
         
       diag_text = "New Event:  "+event_data["event"] +"  Data: "+json.dumps(event_data)
       print "diag_text  ",diag_text, label

       try:
           
           temp = json.loads( card_node.properties["new_commit"] )
           if type(temp) is not list:
               print "is not a list"
               temp = []
       except:
           print "exception"
           temp = []
       temp.append(diag_text)
       print "temp",temp
       print "len",len(temp)
       card_node.properties["new_commit"] = json.dumps(temp)
       card_node.properties["label"] = label
       card_node.push()
 

              
   def get_cards( self, controller, cards_actions ):       
       return_value = {}
       for i in cards_actions.keys():
           card_name = cards_actions[i]["card"]
           card = self.qc.match_relation_property_specific( "CONTROLLER","namespace", controller.properties["namespace"] ,"DIAGNOSTIC_CARD","name",card_name )        
           if len(card) > 0 :
               #print "card_name",card_name
               return_value[i] = { "card_node":card[0], "card_action": cards_actions[i] }

       return return_value

   def monitor_event_queue_cf( self, *args ):
       self.monitor_event_queue( 50 )










   def monitor_event_queue( self,search_depth, *args ):
       ref_time = time.time()
       self.rc = Rabbitmq_Remote_Connections()
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
           vhost           = i.properties["vhost"]
           self.rc         = Rabbitmq_Remote_Connections()
           station_control = self.rc.get_station_control(  i.properties["vhost"] )
           queue_list = self.qc.match_relation_property( "CONTROLLER","namespace",  i.properties["namespace"],"EVENT_QUEUE" )
           for j in queue_list:
               cards_actions = json.loads(j.properties["events"] )
               cards = self.get_cards( i, cards_actions )
               redis_queue = j.properties["name"]
               if j.properties["timestamp"] +48*3600 < time.time():
                    timestamp = time.time() - 48*3600
               else:
                    timestamp = j.properties["timestamp"]
               timestamp_max = timestamp
               queue_length = int( station_control.redis_llen([redis_queue])[1][0]["data"] )
               search_interval = range(0,search_depth)
               search_interval.reverse()
               for k in search_interval:
                   data = station_control.redis_lindex(  [{"key":redis_queue,  "index":k } ])
                   if data[0] == True:
                       data_base64 = data[1][0]["data"]
                       data_json  = base64.b64decode( data_base64)
                       data       = json.loads(data_json)
                       
                       if data["time"] > timestamp_max:
                           timestamp_max = data["time"]
                       if timestamp < data["time"]  :
                           #print k,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(data["time"])), data["status"], data["event"]
                           event = data["event"]
                           if cards.has_key(event):
              
                              self.process_card( cards[event], data )
                           else:
                              pass
                              #print k,"no card match"
                       
               print "time_stamps",j.properties["timestamp"],timestamp_max
               j.properties["timestamp"] = timestamp_max
               j.push()




'''
OPEN_MASTER_VALVE
CLOSE_MASTER_VALVE
DIAGNOSTICS_SCHEDULE_STEP_TIME
RESUME_OPERATION
OFFLINE
SKIP_STATION
IRRIGATION:CURRENT_ABORT
IRRIGATION:FLOW_ABORT
'''
'''
755 2015-12-18 17:00:13 store_eto
756 2015-12-17 22:20:27 reboot
'''


if __name__ == "__main__":
   rc                            = Rabbitmq_Remote_Connections()
   qc                            = Query_Configuration()
   cf                            = py_cf.CF_Interpreter()
   redis_startup                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   meq                           = Monitor_Event_Queues( rc, qc, cf  )
   #meq.monitor_cloud_queue()
   meq.monitor_event_queue(25)

