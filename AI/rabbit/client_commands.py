import pika
import uuid
import json
import base64
import os
import time
import redis


class Status_Alert_Cmds():

   def __init__(self  ):
     pass


   def set_rpc( self, rpc_interface ,time_out ):
       self.rpc_interface = rpc_interface
       self.time_out = time_out


   def __init__(self ):
     self.cmds = {}
     self.cmds["PING"]                         = True
     self.cmds["REDIS_GET"]                    = self.redis_get
     self.cmds["REDIS_SET"]                    = self.redis_set
     self.cmds["REDIS_LLEN"]                   = self.redis_llen
     self.cmds["REDIS_LINDEX"]                 = self.redis_lindex
     self.cmds["REDIS_LSET"]                   = self.redis_lset
     self.cmds["REDIS_TRIM"]                   = self.redis_trim
     self.cmds["REDIS_PUSH"]                   = self.redis_lpush
     self.cmds["REDIS_POP"]                    = self.redis_rpop
     self.cmds["REDIS_DEL"]                    = self.redis_del
     self.cmds["REDIS_HGET"]                   = self.redis_hget
     self.cmds["REDIS_HSET"]                   = self.redis_hset
     self.cmds["REDIS_HGET_ALL"]               = self.redis_hget_all    
     self.cmds["REDIS_HDEL"]                   = self.redis_hdel
     
   def  send_command( self, command, data ):
       command_data             = {}
       command_data["command"]  = command
       command_data["data"]     = data
       #print "command data",command_data
       reply  =  self.rpc_interface.call(  command_data, self.time_out)
       reply = self.check_reply( reply )
       return reply
 

   def ping( self ):
        command_data = {}
        command_data["command"] = "PING"
        command_data["data"]    = []
        reply  =  self.rpc_interface.call( command_data, self.time_out)
        reply = self.check_reply( reply )
        return reply


   # array of keys
   # returns
   # array of dictionary where each element has the following elements
   #   key -- key
   #   data -- data
   def redis_get( self, data ): #array of dictionary with index of "key"
        return self.send_command( "REDIS_GET", data )
      

   # array of dictionaries where each element is of the form   
   #        key  = i["key"]
   #        data = i["data"]
   # returns true
   def redis_set( self, data ): #array of dictionary with index of key and data
        return self.send_command( "REDIS_SET", data )
       

   # array of list keys
   # returns an array of dictionaries where each element has the following form
   # array of keys
   def redis_llen( self, command_data ): # array of dictionary with index of key
         return self.send_command( "REDIS_LLEN", command_data )

   # Array of dictionary where each element has the following values
   #       key    = command_data["key"]
   #       index  = int(command_data["index"])
   # returns an array of dictionaries where each element has the following form
   #  "key":key, 
   #  "index":index, 
   #"data":self.redis.lindex( key, index )
   def redis_lindex( self, command_data ): # array of dictionary with index of key and index
         return self.send_command( "REDIS_LINDEX", command_data )

   # Array of dictionary where each element has the following values  
   #       key    = i["key"]
   #       index  = int(i["index"])
   #       value  = i["value"]
   #  returns true
   def redis_lset( self, command_data ): #array of dictionary with index of key index and value
         return self.send_command("REDIS_LSET", command_data )

   # Array of dictionary where each element has the following values   
   #       key      = i["key"]
   #       start    = i["start"]
   #       end      = i["end"]
   # returns true
   def redis_trim( self, command_data ):#array of dictionary of indexes key,start,end
         return self.send_command( "REDIS_TRIM", command_data )

   # Array of dictionary where each element of the dictionary has the following values
   #    key = i["key"]
   #    data = i["data"] # array of data elements to push
   #    returns true
   def redis_lpush( self, command_data ):#array of dictionary of indexes of key, value
         return self.send_command( "REDIS_PUSH", command_data )

   #
   #  Array of dictionary where each element has the following values
   #       key    = i["key"]
   #       number = i["number"]
   #
   # returns array of dictionaries where each dictionary has the following elements
   # "key"
   # "number" #requested number
   # "data" array of data values -- number may be different due to what is in the queue
   def redis_rpop( self, command_data ): #array of dictionary of indexes of key, number
         return self.send_command( "REDIS_POP", command_data )
      
   #array of dictionary keys to  delete
   # returns true
   def redis_del( self, command_data): 
         return self.send_command( "REDIS_DEL", command_data )

   #
   #  Array of dictionary where each element has the following values
   #       hash   = i["hash"]
   #       key    = i["key"]
   #       
   #  return True


   def redis_hdel( self, command_data): 
         return self.send_command( "REDIS_HDEL", command_data )
 


   #
   #  Array of dictionary where each element has the following values
   #       hash   = i["hash"]
   #       key    = i["key"]
   #       
   #
   # returns array of dictionaries where each dictionary has the following elements
   # "hash"
   # "key"
   # "value" 
   

   def redis_hget( self, command_data): 
         return self.send_command( "REDIS_HGET", command_data )

   #
   #  Array of dictionary where each element has the following values
   #       hash   = i["hash"]
   #       key    = i["key"]
   #       value  = i["data"]
   #
   # returns true

   def redis_hset( self, command_data): 
         return self.send_command( "REDIS_HSET", command_data )
   #
   #  Array of dictionary where each element is dictionary key
   #       key    = i["key"]
   #       number = i["number"]
   #
   # returns array of dictionarys
    
   def redis_hget_all( self, command_data):
         return self.send_command( "REDIS_HGET_ALL", command_data )





   # test to see if command matches
   def check_reply( self, reply ):
      #print "reply",reply
      try:
       if reply["command"] == reply["reply"] :
         if reply.has_key("results") == True:
            return_value = [ True, reply["results"] ]
         else:
             return_value = [ True, None] 
         #print "return_value",return_value
       else:
         return_value = [ False, None ]
      except:
         return_value = [ False, None ]
      return return_value

  
if __name__ == "__main__":
   redis_startup         = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   username              = redis_startup.hget("AI_rabbitmq","username" )
   password              = redis_startup.hget("AI_rabbitmq","password" )
   server                = redis_startup.hget("AI_rabbitmq","server"   )
   port                  = int(redis_startup.hget("AI_rabbitmq","port"     ) )
   queue                 = redis_startup.hget("AI_rabbitmq","queue" )
   vhost                 = "LaCima"
   import   rabbitmq_client

   remote_interface      = rabbitmq_client.RabbitMq_Client(server,port,username,password,vhost,queue)
   station_control       = Status_Alert_Cmds( )
   station_control.set_rpc( remote_interface , 10 ) 
   redis               = redis.StrictRedis( host = "127.1.1.1", port=6379, db = 0 )

   print "ping" , station_control.ping()

 
   return_value = station_control.redis_hget([{"hash":"EQUIPMENT_ENVIRON","key":"CONTROLLER" }])
   print "redis_hget",return_value

   return_value = station_control.redis_hget_all([{"hash":"EQUIPMENT_ENVIRON" }])
   print "redis_hget_all",return_value

   #
   #  Array of dictionary where each element has the following values
   #       hash   = i["hash"]
   #       key    = i["key"]
   #       value  = i["data"]
   #
   # returns true

   return_value = station_control.redis_hset([{"hash":"xxxx_yyyy","key":"xxxx","data":45}])
   print "redis_hset",return_value

   return_value = station_control.redis_hget([{"hash":"xxxx_yyyy","key":"xxxx" }])
   print "redis_hget",return_value

   return_value = station_control.redis_hdel([{"hash":"xxxx_yyyy","key":"xxxx" }])
   print "redis_hget",return_value
 
         
   return_value = station_control.redis_hget([{"hash":"xxxx_yyyy","key":"xxxx" }])
   print "redis_hget",return_value
 
   remote_interface.close()










