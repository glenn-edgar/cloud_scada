import pika
import uuid
import json
import base64
import os
import time
import redis
import rabbitmq_client


class Station_Control():
   def __init__( self ):
       pass


   def close( self ):
       self.rpc_interface.close()

   def set_rpc( self, rpc_interface, time_out ):
      self.rpc_interface = rpc_interface
      self.time_out      = time_out


   def get_web_page( self, path ):
      data = {}
      data["command"]                          = "GET_WEB_PAGE" 
      data["path"]                             =  path  
      
      reply                                    =  self.rpc_interface.call(  data, self.time_out)
      if reply.has_key("results"):
            return [ True, reply["results" ] ]                 
      else:
            return [ False ]
            
      


   def post_web_page( self, path, post_data ):
      data = {}
      data["command"]                          =  "POST_WEB_PAGE"
      data["path"]                             =  path
      data["data"]                             =  post_data 
                     
      reply                                    =  self.rpc_interface.call(  data, self.time_out)
      
      if reply.has_key("results"):
            return [ True, reply["results" ] ]                 
      else:
            return [ False ]
            
      

   def ping( self ):
      data = {}
      data["command"] = "PING"
      reply  =  self.rpc_interface.call(  data, self.time_out)
      reply = self.check_reply( reply )
      return reply
   #
   # todo  add redis commands
   # right now we support get/set and list operation
   #

   # test to see if command matches
   def check_reply( self, reply ):
      try:
       if reply["command"] == reply["reply"] :
         return_value = True
       else:
         return_value = False
      except:
         return_value = False
      return return_value

   # test to see if command matches
   def check_read_reply( self, reply ,command):
      try:
       if reply["command"] == command :
         return_value = reply["reply"]
       else:
         return_value = False
      except:
         return_value = False
      return return_value

