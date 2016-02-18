import json
import redis
import math
from neo4j_graph.graph_functions         import Query_Configuration
from rabbit.rabbitmq_client              import RabbitMq_Client
from rabbit.client_commands              import Status_Alert_Cmds        
from configure_rabbit_queues             import Rabbitmq_Remote_Connections
from mongodb.collection_functions import Mongodb_Collection
from mongodb.collection_functions import Mongodb_DataBase
from pymongo import MongoClient
from mongodb_capped_collections import Capped_Collections
from scipy import stats


class Update_Irrigation_Data():
#"log_data:flow_limits:"+schedule_name+":"+sensor_name
#"log_data:coil_limits:"+schedule

   def __init__( self, rabbitmq_remote_connections,query_configuration ,capped_collection):
       self.rc = rabbitmq_remote_connections
       self.qc = query_configuration
       self.cc_max_number   = 512
       self.cc_db_size      = 5000000
       self.cc              = capped_collection

                

   def update_step_help( self,vhost,node, redis_key ):
       pass


   def get_coil_limits( self,vhost,redis_key ):
       
       station_control = self.rc.get_station_control(  vhost)
       result =  station_control.redis_get([redis_key] )
       data =  result[1][0]["data"]
       return json.loads(data)

   def get_flow_limits( self,vhost,redis_key ):
       station_control = self.rc.get_station_control(  vhost)
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
       flow_meters = self.qc.match_relation_property( "CONTROLLER","namespace",controller_node.properties["namespace"],"FLOW_SENSOR")
       return_value = {}
       for i in flow_meters:
          
          name              = i.properties["name"]
          conversion_factor = i.properties["conversion_factor"]
          return_value[name] = float(i["conversion_factor"])
       return return_value

   def update_schedules_flow( self, vhost ,node, key,  flow_limits,conversion_factors ): 
      
       station_control = self.rc.get_station_control(  vhost)
       data = station_control.redis_lindex([{"key":key, "index":0 } ])
       flow_data_json = data[1][0]["data"]
       flow_data      = json.loads( flow_data_json )
       print conversion_factors
       for i in conversion_factors.keys():
           print i, conversion_factors[i]
           factor = float(conversion_factors[i])
           sensor_data = flow_data["fields"][i]
           sensor_data["std"]      =  float(sensor_data["std"])*factor
           sensor_data["average"]  = float(sensor_data["average"])*factor
           sensor_data["total"]    = float(sensor_data["total"]) * factor
           sensor_data["max"]      = float(sensor_data["max"]) * factor
           for j in range(0,len(sensor_data["data"] )):
               sensor_data["data"][j] = float(sensor_data["data"][j]) *factor  

       print sensor_data
       quit()


   
   def update_schedules_current(self, vhost,node, key, number ):
       station_control = self.rc.get_station_control(  vhost)
       index_list = range(0,number)
       index_list.reverse()
       return_value = []
       self.cc.create( key, self.cc_max_number,self.cc_db_size )
       if len( index_list )==0 :
           for i in index_list:
               data = station_control.redis_lindex([{"key":key, "index":i } ])
               current_data = data[1][0]["data"]
               self.cc.insert( key, {"current_data":current_data } )
               return_value.append( current_data)
       else:
               data = station_control.redis_lindex([{"key":key, "index":0 } ])
               current_data = data[1][0]["data"]
               return_value.append( current_data)  
       print "return_value",return_value[-1]         
       node.properties["value"] = return_value[-1]
       
       node.push()
       return return_value
  
  


   def get_flow_data(self, vhost, key, number ):
       station_control = self.rc.get_station_control(  vhost)
       data = station_control.redis_llen([key])
       queue_depth = data[1][0]["data"]
       if number > queue_depth:
           number = queue_depth
       index_list = range(0,number)
       index_list.reverse()
       return_value = {}
      
       if len( index_list ) :
           for i in index_list:
               data = station_control.redis_lindex([{"key":key, "index":i } ])
               flow_data_json = data[1][0]["data"]
               flow_data      = json.loads( flow_data_json )
               time           = flow_data["time"]
               flow_data      = flow_data["fields"]
               for j in flow_data:
                   if return_value.has_key( j ) == False:
                      return_value[j] = []
                   flow_data[j]["time_stamp"] = time
                   return_value[j].append( flow_data[j] )           
       else:
               
               data = station_control.redis_lindex([{"key":key, "index":0 } ])
            
               flow_data_json = data[1][0]["data"]
               flow_data      = json.loads( flow_data_json )
               time           = flow_data["time"]
               flow_data      = flow_data["fields"]
               for i in flow_data:
                   if return_value.has_key( i ) == False:
                      return_value[i] = []
                   flow_data[i]["time_stamp"] = time
                   return_value[i].append( flow_data[i] )  
       
        
       return return_value

   def scale_sensor_data( self, conversion_factor, sensor_data ):
       for i in sensor_data:
           i["max"]      = float(i["max"])*conversion_factor
           i["std"]      = float(i["std"])*conversion_factor
           i["average"]  = float(i["average"])*conversion_factor
           i["total"]    = float(i["total"])*conversion_factor
           avg = i["average"]
           for j in range(0,len(i["data"])):
               i["data"][j] = float(i["data"][j])*conversion_factor
           del i["min"]
           std = 0
           if len(i["data"]) > 6:
               for j in range( 5, len(i["data"])):
                   std = std + ( i["data"][j] - avg)*(i["data"][j]-avg)
               std = math.sqrt( std/(len(i["data"])-5))
           i["std"] = std
           if len(i["data"] ) > 12:
              slope, intercept, r_value, p_value, std_err = stats.linregress(range( 5, len(i["data"])),i["data"][5:])
              i["slope_active"]  = True
              i["slope"]         = slope
              i["intercept"]     = intercept
              i["r_value"]       = r_value
              i["p_value"]       = p_value
              i["std_err"]       = std_err
           else:
              i["slope_active"]  = False
              i["slope"]         = 0
              i["intercept"]     = 0
              i["r_value"]       = 0
              i["p_value"]       = 0
              i["std_err"]       = 0
              
           
           
       return sensor_data
     

   def update_step_data( self, controller_node, schedule_node, step_number, coil_limit_values,flow_limit_values ,number):
        
       vhost = controller_node.properties["vhost"]
       conversion_factors = self.get_conversion_factors(vhost,controller_node )
    
       schedule_name        = schedule_node.properties["name"]
       steps_nodes           = self.qc.match_relation_property_specific( "IRRIGATION_SCHEDULE","namespace", 
                                                                     schedule_node.properties["namespace"],"STEP","name",str(step_number) )
       if len( steps_nodes ) == 0:
           raise  # for now
       step_node = steps_nodes[0]
       step_name      =     step_node.properties["name"]
       flow           =     self.qc.match_relation_property( "STEP","namespace", step_node.properties["namespace"],"FLOW_SENSOR_VALUE" ) 
       current        =     self.qc.match_relation_property( "STEP","namespace", step_node.properties["namespace"],"COIL_CURRENT" )
       current_limit  =     self.qc.match_relation_property( "STEP","namespace", step_node.properties["namespace"],"COIL_CURRENT_LIMIT" )
       self.update_schedules_current( controller_node.properties["vhost"], current[0], "log_data:coil:"+schedule_name+":"+step_name, number ) #current            
       index = int( step_name ) -1
       current_limit[0].properties["limit_avg"] = coil_limit_values[index]["limit_avg"]
       current_limit[0].properties["limit_std"] = coil_limit_values[index]["limit_std"]
       current_limit[0].push()
       flow_data = self.get_flow_data( controller_node.properties["vhost"], "log_data:flow:"+schedule_name+":"+step_name,number)
       for l in flow:  #nodes of flow values
            sensor_name = l.properties["name"]
            l.properties["mongodb_collection"] = "log_data:flow:"+schedule_name+":"+step_name+":"+sensor_name
            conversion_factor = conversion_factors[ sensor_name ]
            sensor_data = flow_data[sensor_name]
            sensor_data = self.scale_sensor_data( conversion_factor, sensor_data )
            l.properties["value"] = json.dumps(sensor_data[-1])
            l.properties["time_stamp"]  = sensor_data[-1]["time_stamp"]
            l.push()
            if number > 0 :
                self.cc.create( l.properties["mongodb_collection"], self.cc_max_number,self.cc_db_size )
                self.cc.insert( l.properties["mongodb_collection"], sensor_data )
            flow_limits        = self.qc.match_relation_property( "STEP","namespace", step_node.properties["namespace"],"FLOW_SENSOR_LIMIT" )  
            for l in flow_limits:
                name = l.properties["name"]
                l.properties["limit_avg"] = float(flow_limit_values[name][index]["limit_avg"])*conversion_factors[name]
                l.properties["limit_std"] = float(flow_limit_values[name][index]["limit_std"])*conversion_factors[name]
                l.push() 



   # from event stream
   def update_irrigation_step( self,controller_node, schedule_name, step_number,number ):
       schedule_node = self.qc.match_relation_property_specific( "CONTROLLER","namespace", controller_node.properties["namespace"],"IRRIGATION_SCHEDULE","name",schedule_name )
       if len( schedule_node) == 0:
            raise
       vhost = controller_node.properties["vhost"]
       coil_limit_values    =  self.get_coil_limits( vhost,"log_data:coil_limits:"+schedule_name )
       flow_limit_values    =  self.get_flow_limits( vhost,"log_data:flow_limits:"+schedule_name )
       self.update_step_data( controller_node, schedule_node[0], step_number, coil_limit_values,flow_limit_values,number )


   def update_irrigation_data( self , number):
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
           vhost = i.properties["vhost"]
           conversion_factors = self.get_conversion_factors(vhost,i )
           
           schedule_list = self.qc.match_relation_property( "CONTROLLER","namespace",i.properties["namespace"],"IRRIGATION_SCHEDULE")
     
           for j in schedule_list:
               schedule_name        = j.properties["name"]
               coil_limit_values    =  self.get_coil_limits( vhost,"log_data:coil_limits:"+schedule_name )
               flow_limit_values    =  self.get_flow_limits( vhost,"log_data:flow_limits:"+schedule_name )
               steps                = self.qc.match_relation_property( "IRRIGATION_SCHEDULE","namespace", j.properties["namespace"],"STEP" )
               for k in steps:
                   step_name      =     k.properties["name"]
                   flow           =     self.qc.match_relation_property( "STEP","namespace", k.properties["namespace"],"FLOW_SENSOR_VALUE" ) 
                   current        =     self.qc.match_relation_property( "STEP","namespace", k.properties["namespace"],"COIL_CURRENT" )
                   current_limit  =     self.qc.match_relation_property( "STEP","namespace", k.properties["namespace"],"COIL_CURRENT_LIMIT" )

                   
                   self.update_schedules_current( i.properties["vhost"], current[0], "log_data:coil:"+schedule_name+":"+step_name, number ) #current
                   
                   index = int( step_name ) -1
                   current_limit[0].properties["limit_avg"] = coil_limit_values[index]["limit_avg"]
                   current_limit[0].properties["limit_std"] = coil_limit_values[index]["limit_std"]
                   current_limit[0].push()

                   flow_data = self.get_flow_data( i.properties["vhost"], "log_data:flow:"+schedule_name+":"+step_name,number)
                   for l in flow:  #nodes of flow values
                       sensor_name = l.properties["name"]
                       l.properties["mongodb_collection"] = "log_data:flow:"+schedule_name+":"+step_name+":"+sensor_name
                       conversion_factor = conversion_factors[ sensor_name ]
                       sensor_data = flow_data[sensor_name]
                       sensor_data = self.scale_sensor_data( conversion_factor, sensor_data )
                       l.properties["value"] = json.dumps(sensor_data[-1])
                       l.properties["time_stamp"]  = sensor_data[-1]["time_stamp"]
                       l.push()
                       if number > 0 :
                          self.cc.create( l.properties["mongodb_collection"], self.cc_max_number,self.cc_db_size )
                          self.cc.insert( l.properties["mongodb_collection"], sensor_data )
                       
                       
                      
                      

                   
                   

                   flow_limits        = self.qc.match_relation_property( "STEP","namespace", k.properties["namespace"],"FLOW_SENSOR_LIMIT" )  
                   for l in flow_limits:
                       name = l.properties["name"]
                       l.properties["limit_avg"] = float(flow_limit_values[name][index]["limit_avg"])*conversion_factors[name]
                       l.properties["limit_std"] = float(flow_limit_values[name][index]["limit_std"])*conversion_factors[name]
                       
                       l.push() 
                       
if __name__ == "__main__":

   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
 
   rc          = Rabbitmq_Remote_Connections()
   qc          = Query_Configuration()
   cc          = Capped_Collections( mongodb_db, mongodb_col, db_name = "Capped_Colections" ) 
   idd         = Update_Irrigation_Data(rc,qc,cc)
   #idd.update_irrigation_data(0)
   controller_list = qc.match_labels("CONTROLLER")
   for i in controller_list:
      idd.update_irrigation_step( i, "house", str(1) ,0)
                
