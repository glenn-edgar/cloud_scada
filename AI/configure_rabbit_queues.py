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

   def do_all( self, function ):
       vhost_list = self.rc.connections.keys()
       return_result = []
       for i in vhost_list:
          result = function(i)
          return_result.append( {"queue":i,"result":result[0] } )
       return return_result


#"log_data:resistance_log_limit:"+controller_name+":"+valve_list[j]
   def get_coil_value_function_1( self, station_control, key):
       data = station_control.redis_lindex([{"key":key, "index":0 } ])
       return data[1][0]["data"]
 
   def get_coil_value_function_2( self, station_control, key):
       data = station_control.redis_get([key])
       return data[1][0]["data"]
       

   def get_coil_current_help_a( self, vhost, valve_list, key_pattern, value_function ):
       station_control = rc.get_station_control(  vhost)
       valid_stations = station_ctl.redis_keys([{"key":key_pattern} ] )
       valid_stations = valid_stations[1][0]
       valid_list = []
    
       for i in valid_stations:
           
           temp_list = i.split(":")
           valid_list.append(temp_list[-1])

       for i in valve_list:
           if i.properties["name"] in valid_list:
               index = valid_list.index(i.properties["name"])
               if value_function != None:
                   i.properties["value"] = value_function(station_control,valid_stations[index])
               i.properties["active"] = True
           else:
               i.properties["active"] = False
          
           i.push()
        
   
   def get_coil_current( self  ):
       controller_list = qc.match_labels("CONTROLLER")
       for i in controller_list:
           remote_list = qc.match_relation_property( "CONTROLLER","name", i.properties["name"],"REMOTE" )
           for j in remote_list:
               valve_list       = qc.match_relation_property( "REMOTE","name", j.properties["name"],"IRRIGATION_VALVE_CURRENT" )
               self.get_coil_current_help_a( i.properties["vhost"], valve_list,"log_data:resistance_log:"+j.properties['name']+":*",self.get_coil_value_function_1)
               valve_list       = qc.match_relation_property( "REMOTE","name", j.properties["name"],"IRRIGATION_VALVE_CURRENT_LIMIT" )
               self.get_coil_current_help_a( i.properties["vhost"], valve_list,"log_data:resistance_log_limit:*"+j.properties['name']+":*",self.get_coil_value_function_2) 
                

   def update_step_help( self,vhost,node, redis_key ):
       pass


   def get_coil_limits( self,vhost,redis_key ):
       
       station_control = rc.get_station_control(  vhost)
       result =  station_control.redis_get([redis_key] )
       data =  result[1][0]["data"]
       return json.loads(data)

   def get_flow_limits( self,vhost,redis_key ):
       station_control = rc.get_station_control(  vhost)
       key_list = redis_key+":*"
       result = station_control.redis_keys([{"key":key_list}])
       flow_meter_keys = result[1][0]

       return_value = {}
       for i in flow_meter_keys:
          redis_fields = i.split(":")
          result = station_control.redis_get([i])
          return_value[redis_fields[-1]] = json.loads(result[1][0]["data"])
       
       return return_value

   def get_conversion_factors( self, vhost, controller_node ):
       flow_meters = qc.match_relation_property( "CONTROLLER","workspace_name",controller_node.properties["workspace_name"],"FLOW_SENSOR")
       return_value = {}
       for i in flow_meters:
          
          name              = i.properties["name"]
          conversion_factor = i.properties["conversion_factor"]
          return_value[name] = float(i["conversion_factor"])
       return return_value

   def update_schedules( self ):
       controller_list = qc.match_labels("CONTROLLER")
       for i in controller_list:
           vhost = i.properties["vhost"]
           conversion_factors = self.get_conversion_factors(vhost,i )
           schedule_list = qc.match_relation_property( "CONTROLLER","workspace_name",i.properties["workspace_name"],"IRRIGATION_SCHEDULE")
           for j in schedule_list:
               schedule_name        = j.properties["name"]
               coil_limit_values    =  self.get_coil_limits( vhost,"log_data:coil_limits:"+schedule_name )
               flow_limit_values    =  self.get_flow_limits( vhost,"log_data:flow_limits:"+schedule_name )
               
               steps               = qc.match_relation_property( "IRRIGATION_SCHEDULE","workspace_name", j.properties["workspace_name"],"STEP" )
               for k in steps:
                   step_name      =     k.properties["name"]
                   flow           =     qc.match_relation_property( "STEP","workspace_name", k.properties["workspace_name"],"FLOW" ) 
                   current        =     qc.match_relation_property( "STEP","workspace_name", k.properties["workspace_name"],"COIL_CURRENT" )
                   current_limit  =     qc.match_relation_property( "STEP","workspace_name", k.properties["workspace_name"],"COIL_CURRENT_LIMIT" )
                   
                   #self.update_schedules_help( i.properties["vhost"],flow, "log_data:flow:"+schedule_name+":"+step_name) #flow
                   #self.update_schedules_help( i.properties["vhost"],current, "log_data:coil:"+schedule_name+":"+step_name) #current
                 
                   index = int( step_name ) -1
                   current_limit[0].properties["limit_avg"] = coil_limit_values[index]["limit_avg"]
                   current_limit[0].properties["limit_std"] = coil_limit_values[index]["limit_std"]
                   current_limit[0].push()

                   flow_limits        = qc.match_relation_property( "STEP","workspace_name", k.properties["workspace_name"],"FLOW_SENSOR_LIMIT" )  
                   for l in flow_limits:
                       name = l.properties["name"]
                       l.properties["limit_avg"] = float(flow_limit_values[name][index]["limit_avg"])*conversion_factors[name]
                       l.properties["limit_std"] = float(flow_limit_values[name][index]["limit_std"])*conversion_factors[name]
                       print l.properties
                       l.push() 
                       
                


   
if __name__ == "__main__":

 
   rc         = Rabbitmq_Remote_Connections()
   qc              = Query_Configuration()
   a_r_redis  = Access_Remote_Redis_Structures(rc,qc)
   stations   =  rc.get_stations()
   station_ctl = rc.get_station_control( stations[0] )

   print a_r_redis.ping_a_station(stations[0])
   #a_r_redis.get_coil_current()
   a_r_redis.update_schedules()
