import json
import redis
from neo4j_graph.graph_functions         import Query_Configuration
from rabbit.rabbitmq_client              import RabbitMq_Client
from rabbit.client_commands              import Status_Alert_Cmds        
from configure_rabbit_queues             import Rabbitmq_Remote_Connections



class Update_Irrigation_Data():
#"log_data:flow_limits:"+schedule_name+":"+sensor_name
#"log_data:coil_limits:"+schedule

   def __init__( self, rabbitmq_remote_connections,query_configuration ):
       self.rc = rabbitmq_remote_connections
       self.qc = query_configuration

                

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
   qc         = Query_Configuration()
   idd         = Update_Irrigation_Data(rc,qc)
   idd.update_irrigation_data()
                
