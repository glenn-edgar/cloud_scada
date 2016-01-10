# external control 
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

from  client_commands                 import *
from  rabbit_alert_connection_control import *
from  remote_message_handlers         import *
from  state_management                import *
import system_topology_manager

#
#
# Global Varibles
#
#
   redis_startup         = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   username              = redis_startup.hget("AI_rabbitmq","username" )
   password              = redis_startup.hget("AI_rabbitmq","password" )
   server                = redis_startup.hget("AI_rabbitmq","server"   )
   port                  = int(redis_startup.hget("AI_rabbitmq","port"     ) )
   queue                 = redis_startup.hget("AI_rabbitmq","queue" )




class Rabbit_RPC_Base:

   def __init__( self ):
       pass

   # test to see if command matches
   def check_reply( self, reply ):
      #print "reply",reply
      try:
       if reply["command"] == reply["reply"] :
         if reply.has_key("results") == True:
            return_value = [ True, reply["results"] ]
         else:
             return_value = [ True, None] # no attached data
         #print "return_value",return_value
       else:
         return_value = [ False, None ]  #command did not match
      except:
         return_value = [ False, None ]  #exception results
      return return_value
      
   def set_server_connections( server,port, username ,password)
       self.username = username
       self.password = password
       self.port     = port
       self.server   = server


   def  send_command( self, vhost = None, rpc_queue = None, time_out=None,command=None, data=None ):
       command_data             = {}
       command_data["command"]  = command
       command_data["data"]     = data
       #print "command data",command_data
       reply  =  self.rpc_interface.call(  command_data, time_out )
       reply = self.check_reply( reply )
       connection_class = RabbitMq_Client( self.server,self.port,self.username,self.password,vhost, rpc_queue )
       result = connection_class.call( data, time_out )
       connection_class.close()
       connection_class = None   # force garbage collection
       reply = self.check_reply(result)
       return reply

   def pop_queue( self,vhost_list=None, remote_queue=remote_queue, down_load_number= down_load_number):
       return_value = {}
       for vhost in vhost_list:
           temp = self.send_command("REDIS_POP", {"key":remote_queue, "number":down_load_number })
           if temp[0] == True:
               return_value[vhost] = temp[1]
           else
               return_value[vhost] = None
   
 


class Analyize_Alarms:


   def process_alarms( self, *args ):
       pass

class Analyize_Events:

   def __init__( self,queue):
       self.queue = queue

   def down_load_events( self,*args):
       pass

   def process_events( self, *args ):
       pass





if __name__ == "__main__":



   redis                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 0 )
   #construct graphical data base
  
   #instanciate classes

   #define chains

   #execute chain flow
   cf.define_chain( "master_sequencer", True )    ## auto start thread 
   # cf.insert_link( "link_1","One_Step",[ send_reboot_message ] )
   cf.insert_link( "link_4","Disable_Chain",[["master_sequencer"]]) 

   cf.define_chain("Check Alarms",False)
   cf.insert_link( "link_1",  "WaitTime",    [30,0,0,0] )  # wait to 30 seconds
   #  cf.insert_link( "link_2","One_Step",[ analyize_event ] )
   cf.insert_link( "link_3","Reset",[] )
 
   cf.define_chain("Check Events",False)
   cf.insert_link( "link_1",  "WaitTime",    [30,0,0,0] )  # wait to 30 seconds  
   #  cf.insert_link( "link_2","One_Step",[ analyize_events ] )
   cf.insert_link( "link_3","Reset",[] )
    
   cf.define_chain("Check ETO", False )

   cf.define_chain("Check Coil Resistance",False)

   cf.define_chain("Check Slave Statistics",False)


  
 





#  cf = py_cf.CF_Interpreter()
  


'''



class Check_Messages():

  def __init__(self, queue, time_out ,clients ):
      self.clients               = clients
      self.time_out              = time_out
      self.queue                 = queue
      self.connection_control    = Connection_Control(queue )  # rabbitmq client

      
       
  def down_load_remote_data( self, chainFlowHandle, chainObj, parameters, event ):
      for i in self.clients:
#        try:
#          temp_station = self.connection_control.get_station_control( i, self.time_out, clients[i]["message_fetch"] )
#          try:
#           # create rabbit queue connection
#        
#           # down load and process messages to handle events
#            self.clients[i]["message_process"]( chainFlowHandle, chainObj, parameters, event,  temp_station )
#          
#          except:
#             raise          
#             print "exception processing message"
#             
#             self.connection_control.close(i)
#        except:
#          raise
#          print "exception in getting connection"

        
          temp_station = self.connection_control.get_station_control( i, self.time_out, clients[i]["message_fetch"] )
          self.clients[i]["message_process"]( chainFlowHandle, chainObj, parameters, event,  temp_station )
          self.connection_control.close(i) 

       

  def poll_aggregators( self, chainFlowHandle, chainObj, parameters, event ):
      for i in self.clients:
       try:
         self.clients[i]["poll_aggregators"](  )
       except:
         pass
         #print "poll aggregators exceptions"
         #quit()
  
#
# Need to handle handlers
#
#


if __name__ == "__main__":


  cf_ext = Chain_Flow_Extensions()
  redis                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 0 )
  contact_manager       = Contact_Manager( redis, "routing_group.json","contact_manager.json") #Contact_Manager(redis,"routing_group.json","contact_manager.json")
  ic_dl   = Irrigation_Controller_1_Data_Store(30,"irrigation_alarm_event_queue","alarm_event_queue","LaCima",redis,contact_manager)

  sd                = {}
  sd["LaCima"]                   = Analyize_Sprinkler_Data( "LaCima",redis, contact_manager, ["fruit_trees_low_water"] )
  sd["LaCima"].construct_assert_tree()
   
  clients = {}
  clients["LaCima"] = {   "message_fetch": Status_Alert_Cmds(), 
                          "message_process": ic_dl.process_message, 
                          "poll_aggregators":ic_dl.routing_group.poll_aggregators
                           # add sprinkler analysis software 
                      }
  conect_ctrl       = Check_Messages( 'alert_status_queue',10, clients)
  
  def check_lacima_trees(  ):
    print "check lacima trees"
    x =sd["LaCima"]
    input_data = {}
    input_data["schedule"]    = "fruit_trees_low_water"
    input_data["step"]        = 1
    input_data["number"]      = 50
    input_data["latest_time"] = 24*3600*60
    for i in range(1,12):
          input_data["step"]  = i
          output_data = {}
          return_value = x.assert_tree.process( input_data,output_data)
          print "return_value",return_value


  def check_trees(chainFlowHandle, chainObj, parameters, event ):
    check_lacima_trees()


  # Adding chains
  #
  cf = py_cf.CF_Interpreter()
  
#  
# ETO processing elements  
# 
#  cf.define_chain( "master_sequencer", True )    ## auto start thread 
#  cf.insert_link( "link_3", "Enable_Chain",[["new_day_house_keeping","get_current_eto","delete_cimis_email_data" ]])
#  cf.insert_link( "link_4","Disable_Chain",[["master_sequencer"]]) 



  cf.define_chain("Parse_Message",True)
  cf.insert_link( "link_1","Code",[cf_ext.time_event_generator,300,0 ] ) # down load every 5 minutes
  cf.insert_link( "link_2","One_Step",[conect_ctrl.down_load_remote_data] )
  cf.insert_link( "link_3","Reset",[] )
 
  #this is output to send alarms
  cf.define_chain("Check_Aggregators",True)
  cf.insert_link( "link_1","Code",[cf_ext.time_event_generator,30,0 ] ) # down load every 30 second
  cf.insert_link( "link_2","One_Step",[conect_ctrl.poll_aggregators] )
  cf.insert_link( "link_3","Reset",[] )

  cf.define_chain("Check_State",True)
  cf.insert_link( "link_1","WaitTod",["*",15,"*","*" ])
  #cf.insert_link( "link_2","One_Step",[sm.check_system_state ] )
  cf.insert_link( "link_3","WaitTod",["*",16,"*","*" ])
  cf.insert_link( "link_4","Reset",[] )

  cf.define_chain("Check_Trees",True)
  cf.insert_link( "link_1","WaitTod",["*","*",5,"*" ])
  cf.insert_link( "link_2","One_Step",[check_trees ] )
  cf.insert_link( "link_3","WaitTod",["*","*",30,"*" ])
  cf.insert_link( "link_4","Reset",[] )

#
#  chains for handling reboot events
#
#
#
#  cf.define_chain("Handle_Reboot_Event",True)
#  cf.insert_link( "link_1",  "WaitEvent",[ "REBOOT_ALARM"  ] )
#  cf.insert_link( "link_2",  "Log",[ "got Reboot Alarm" ] )
#  cf.insert_link( "link_3",  "Code",[check_alarms.process_reboot_message ] )
#  cf.insert_link( "link_3",  "Reset",[] )



#
# Add chains for event handlers
#
#
#



  #
  # Executing chains
  #
  cf_environ = py_cf.Execute_Cf_Environment( cf )
  cf_environ.execute()



