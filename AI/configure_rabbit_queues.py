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


class Access_Remote_Redis_Structures():
#"log_data:flow_limits:"+schedule_name+":"+sensor_name
#"log_data:coil_limits:"+schedule

   def __init__( self, rabbitmq_remote_connections,query_configuration ):
       self.rc = rabbitmq_remote_connections
       self.qc = query_configuration

   def ping_a_station( self,vhost):
       station = self.rc.get_station_control(vhost)
       return station.ping()[0]

   def ping_all_station( self ):
       vhost_list = self.rc.connections.keys()
       return_result = []
       for i in vhost_list:
          station = self.rc.get_station_control(i)
          result = station.ping()
          return_result.append( {"queue":i,"result":result[0] } )
       return return_result

#"log_data:resistance_log_limit:"+controller_name+":"+valve_list[j]   
   def get_coil_resistance( self ):
       controller_list = qc.match_labels("CONTROLLER")
       for i in controller_list:
           remote_list = qc.match_relation_property( "CONTROLLER","name", i.properties["name"],"REMOTE" )
           for j in remote_list:
               irrigation_valve_current       = qc.match_relation_property( "REMOTE","name", j.properties["name"],"IRRIGATION_VALVE_CURRENT" )
                
               # form key search "log_data:resistance_log_limit:"+j.properties["name"]+":"*
               # form array of numbers array of numbers
               # convert array of numbers to a set
                
               valid_set = set([])             

               for k in irrigation_valve_current:
                   print k.properties["name"],k.properties["workspace_name"]
                   if str( k.properties["name"] ) in valid_set:
                       k.properties["active"]    = True
                       
                       print "made it here set",k.properties["name"],k.properties["workspace_name"]
                       # do look up and set current
                       # do look up and set current limit
                   else:
                       k.properties["active"]    = False
                       
                   k.push()

               irrigation_valve_current       = qc.match_relation_property( "REMOTE","name", j.properties["name"],"IRRIGATION_VALVE_CURRENT_LIMIT" )
                
               # form key search "log_data:resistance_log_limit:"+j.properties["name"]+":"*
               # form array of numbers array of numbers
               # convert array of numbers to a set
                
               valid_set = set([])             

               for k in irrigation_valve_current:
                   print k.properties["name"],k.properties["workspace_name"]
                   if str( k.properties["name"] ) in valid_set:
                       k.properties["active"]    = True
                       
                       print "made it here set",k.properties["name"],k.properties["workspace_name"]
                       # do look up and set current
                       # do look up and set current limit
                   else:
                       k.properties["active"]    = False
                       
                   k.push()
                  
                
                    




if __name__ == "__main__":

 
   rc         = Rabbitmq_Remote_Connections()
   qc              = Query_Configuration()
   a_r_redis  = Access_Remote_Redis_Structures(rc,qc)
   stations   =  rc.get_stations()
   station_ctl = rc.get_station_control( stations[0] )
   print a_r_redis.ping_a_station(stations[0])
   print a_r_redis.ping_all_station()
   print station_ctl.redis_hget_all([{"hash":"CONTROL_VARIABLES"}])
   a_r_redis.get_coil_resistance()
