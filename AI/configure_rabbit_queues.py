#
#  File: configure_rabbit_queues.py
#  This file finds the list of rabbitmq queues for polling
#  and places it in a redis_list
#
#
#
#
#

import json
import redis
from neo4j_graph.graph_functions         import Query_Configuration
from rabbit.rabbitmq_client              import RabbitMq_Client
from rabbit.client_commands              import Status_Alert_Cmds        

class Rabbitmq_Remote_Connections():
    
   def __init__( self ):
       redis_startup    = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 ) 
       username         = redis_startup.hget("AI_rabbitmq","username" )
       password         = redis_startup.hget("AI_rabbitmq","password" )
       server           = redis_startup.hget("AI_rabbitmq","server"   )
       port             = int(redis_startup.hget("AI_rabbitmq","port"     ) )   
       qc               = Query_Configuration()
       objects          = qc.match_labels( "CONTROLLER" )
       self.station_control       = Status_Alert_Cmds( )
       self.connections = {}
       for i in objects:
           temp = {}
           vhost                     = i.properties["vhost"]
           queue                     = i.properties["rpc_queue"]
           temp["vhost"]             = vhost
           temp["rpc_queue"]         = queue
           temp["workspace_name"]    = i.properties["workspace_name"]
           remote_interface          = RabbitMq_Client(server,port,username,password,vhost,queue)
           station_control           = Status_Alert_Cmds()
           station_control.set_rpc( remote_interface , 10 ) 
           temp["station_control"] = station_control
           self.connections[vhost] = temp

   def get_station_control( self , vhost):
       return self.connections[vhost]["station_control"]

   def get_stations( self ):
       return self.connections.keys()

   def ping_a_station( self,vhost):
       station = self.get_station_control(vhost)
       return station.ping()[0]

   def ping_all( self ):
       vhost_list = self.connections.keys()
       return_result = {}
       for i in vhost_list:
          return_result[i] = self.ping_a_station(i)
       return return_result



   
if __name__ == "__main__":

 
   rc         = Rabbitmq_Remote_Connections()
   stations   =  rc.get_stations()
   print rc.ping_a_station( stations[0] )
   print rc.ping_all()
   

