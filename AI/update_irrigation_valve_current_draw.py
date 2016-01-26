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
from configure_rabbit_queues             import Rabbitmq_Remote_Connections



class Update_Irrigation_Valve_Current_Draw():

   def __init__( self, rabbitmq_remote_connections, query_configuration ):
       self.rc = rabbitmq_remote_connections
       self.qc = query_configuration



#"log_data:resistance_log_limit:"+controller_name+":"+valve_list[j]
   def get_coil_value_function_1( self, station_control, key):
       data = station_control.redis_lindex([{"key":key, "index":0 } ])
       return data[1][0]["data"]
 
   def get_coil_value_function_2( self, station_control, key):
       data = station_control.redis_get([key])
       return data[1][0]["data"]
       

   def get_coil_current_help_a( self, vhost, valve_list, key_pattern, value_function ):
       station_control = rc.get_station_control(  vhost)
       valid_stations = station_control.redis_keys([{"key":key_pattern} ] )
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
        
   
   def update_coil_current( self  ):
       controller_list = qc.match_labels("CONTROLLER")
       for i in controller_list:

      
           remote_list = qc.match_relation_property( "CONTROLLER","name", i.properties["name"],"REMOTE" )
           for j in remote_list:

               valve_list       = qc.match_relation_property( "REMOTE","name", j.properties["name"],"IRRIGATION_VALVE_CURRENT" )
               self.get_coil_current_help_a( i.properties["vhost"], valve_list,"log_data:resistance_log:"+j.properties['name']+":*",self.get_coil_value_function_1)

               valve_list       = qc.match_relation_property( "REMOTE","name", j.properties["name"],"IRRIGATION_VALVE_CURRENT_LIMIT" )
               self.get_coil_current_help_a( i.properties["vhost"], valve_list,"log_data:resistance_log_limit:*"+j.properties['name']+":*",self.get_coil_value_function_2) 
               

               


   
if __name__ == "__main__":

 
   rc         = Rabbitmq_Remote_Connections()
   qc         = Query_Configuration()
   cd         = Update_Irrigation_Valve_Current_Draw(rc,qc)
   cd.update_coil_current()
   
