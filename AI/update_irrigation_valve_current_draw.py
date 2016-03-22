#
#  File: configure_rabbit_queues.py
#  This file finds the list of rabbitmq queues for polling
#  and places it in a redis_list
#
#
#
#
#
import time
import copy
import json
import redis
from slacker                              import Slacker
from media_drivers.trello_management      import Trello_Management 
from neo4j_graph.graph_functions         import Query_Configuration
from rabbit.rabbitmq_client              import RabbitMq_Client
from rabbit.client_commands              import Status_Alert_Cmds        
from configure_rabbit_queues             import Rabbitmq_Remote_Connections
from mongodb.collection_functions import Mongodb_Collection
from mongodb.collection_functions import Mongodb_DataBase
from pymongo import MongoClient
from mongodb_capped_collections import Capped_Collections
from populate_trello_data_base            import Transfer_Data

class Update_Irrigation_Valve_Current_Draw():

   def __init__( self, rabbitmq_remote_connections, query_configuration, mongodb_database ):
       self.rc = rabbitmq_remote_connections
       self.qc = query_configuration
       self.cc = mongodb_database
       self.cc_max_number = 128
       self.cc_db_size    = 10000

   def get_coil_value_function_1_helper( self, station_control,key, index, time_stamp ):
       if time_stamp == None:
          time_stamp = 0 # forces always to retreive last function
       data = station_control.redis_lindex([{"key":key, "index":index } ])
       current_data = data[1][0]["data"]  
       try:
           
           current_data = json.loads( current_data)
           key_list = current_data.keys()
           for i in key_list:
              current_data[i] = float( current_data[i] )
           
           
           if  current_data["time"] > time_stamp:
              
              self.cc.insert( key,  {"current_data":current_data["current"] }  )
              
              return_value =  current_data["current"] 
              
           else:
              return_value = current_data["current"]
       except:
           return_value = None
       
       return return_value


#"log_data:resistance_log_limit:"+controller_name+":"+valve_list[j]
   def get_coil_value_function_1( self, station_control, key, number, time_stamp):  # valve function for storing current
 
       if number != 0:
           index_list = range(0,number)
           index_list.reverse()
       else:
           index_list=[0]
       
       return_value = []
       self.cc.create( key, self.cc_max_number,self.cc_db_size )
       if len( index_list ) :
           for i in index_list:
              index_value = self.get_coil_value_function_1_helper(station_control,key, i, time_stamp )
              if index_value != None:
                 return_value.append( self.get_coil_value_function_1_helper(station_control,key, i, time_stamp ) ) 
       else:
            index_value = self.get_coil_value_function_1_helper(station_control,key, 0, time_stamp )
            if index_value != None:
                return_value.append( self.get_coil_value_function_1_helper(station_control,key, 0, time_stamp ) ) 
       return [return_value[0]]

   def get_coil_value_function_2( self, station_control, key, number= None, time_stamp = None):
       data = station_control.redis_get([key])
       return data[1][0]["data"]
       

   def get_coil_current_help_a( self, vhost, valve_list, key_pattern, value_function, number = None ):
       station_control = self.rc.get_station_control(  vhost)
       valid_stations = station_control.redis_keys([{"key":key_pattern} ] ) # key pattern allows the valves for remotes to be obtained
       valid_stations = valid_stations[1][0]  # get the number of valves attached to a remote
       valid_list = []
       for i in valid_stations:
           
           temp_list = i.split(":") # getting the pin number
           valid_list.append(temp_list[-1])
       for i in valve_list:

           if i.properties.has_key("time_stamp") == False:
               i.properties["time_stamp"] = time.time()
           if i.properties["name"] in valid_list:
               index = valid_list.index(i.properties["name"])
           
               if value_function != None:
                   i.properties["value"] = value_function( station_control,valid_stations[index] , number, i.properties["time_stamp"])
               i.properties["active"] = True
               i.properties["mongodb_collection"] = valid_stations[index]
           else:
               i.properties["active"]             = False
               i.properties["mongodb_collection"] = None
          
           i.properties["time_stamp"] = time.time()
           i.push()
 
   def update_coil_current_cf( self ,*args  ):
       self.update_coil_current(3)
   
   def update_coil_current( self ,number  ):
       self.rc = Rabbitmq_Remote_Connections()
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:

      
           remote_list = self.qc.match_relation_property( "CONTROLLER","name", i.properties["name"],"REMOTE" )
           for j in remote_list:
               
               valve_list       = self.qc.match_relation_property( "REMOTE","namespace", j.properties["namespace"],"IRRIGATION_VALVE_CURRENT" )
               self.get_coil_current_help_a( i.properties["vhost"], valve_list,"log_data:resistance_log_cloud:"+j.properties['name']+":*",self.get_coil_value_function_1, number)

               valve_list       = self.qc.match_relation_property( "REMOTE","namespace", j.properties["namespace"],"IRRIGATION_VALVE_CURRENT_LIMIT" )
               self.get_coil_current_help_a( i.properties["vhost"], valve_list,"log_data:resistance_log_limit:*"+j.properties['name']+":*",self.get_coil_value_function_2) 
               

class Analyize_Valve_Current_Data:

   def __init__( self, query_control ):
       self.qc = query_control

         

   def analyize_data( self,*args ):
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
           remote_list = self.qc.match_relation_property( "CONTROLLER","name", i.properties["name"],"REMOTE" )
           for j in remote_list:
               
               card_dict                = json.loads( j.properties["card_dict"] )
               valve_list_current       = self.qc.match_relation_property( "REMOTE","name", j.properties["name"],"IRRIGATION_VALVE_CURRENT" )
               valve_list_limit         = self.qc.match_relation_property( "REMOTE","name", j.properties["name"],"IRRIGATION_VALVE_CURRENT_LIMIT" )
               open_card                = self.qc.match_relation_property_specific( "CONTROLLER","name",i.properties["name"],"DIAGNOSTIC_CARD","name",card_dict["open"] )[0]
               short_card               = self.qc.match_relation_property_specific( "CONTROLLER","name",i.properties["name"],"DIAGNOSTIC_CARD","name",card_dict["short"] )[0]
               error_flag               = False
               text = "Controller: "+i.properties["name"]+ " Remote: "+j.properties["name"]+"\n"
               update_flag = False
               
               if len( valve_list_current ) == len( valve_list_limit ):
                     
                   for k in range(0,len(valve_list_current )):
                       
                       if valve_list_current[k].properties["active"] == True: 
                           
                           
                           if  len( valve_list_current[k].properties["value"]) != 0:
                               update_flag = True
                               value = valve_list_current[k].properties["value"][0]
                               limit = valve_list_limit[k].properties["value"]
                           
                               if float(value)-float(limit) < -1.0:
                                  error_flag = True
                                  text = text +"**Step: "+str(k)+"  Value: "+str(value)+ "  Limit: "+str(limit)+"  ERROR**\n"
                       
                               else:
                                  text = text +"Step: "+str(k)+"  Value: "+str(value)+ "  Limit: "+str(limit)+"  OK\n"
               
                   if update_flag == True:             
                       try:
                           temp = json.loads( open_card.properties["new_commit"] )
                           if type(temp) is not list:
                               temp = []
                       except:
                           temp = []
               
                       temp.append( text)
                       open_card.properties["new_commit"] = json.dumps(temp)
                       if error_flag == True:
                           open_card.properties["label"] = "red"
                       else:
                           open_card.properties["label"] = "green"
                       print "color",open_card.properties["label"]
                       open_card.push()      

               update_flag = False
               error_flag               = False
               text = "Controller: "+i.properties["name"]+ " Remote: "+j.properties["name"]+"\n"
               if len( valve_list_current ) == len( valve_list_limit ):
                   for k in range(0,len(valve_list_current )):
                        
                       if valve_list_current[k].properties["active"] == True:
                           
                           if len(valve_list_current[k].properties["value"]) != 0:
                              update_flag = True
                              value = valve_list_current[k].properties["value"][0]
                              limit = valve_list_limit[k].properties["value"]
                              if float(value)-float(limit) > 3.0:
                                  error_flag = True
                                  text = text +"**Step: "+str(k)+"  Value: "+str(value)+ "  Limit: "+str(limit)+"  ERROR**\n"
                              else:
                                  text = text +"Step: "+str(k)+"  Value: "+str(value)+ "  Limit: "+str(limit)+"  OK\n"
               
               
                   if update_flag == True:             
                       try:
                           temp = json.loads( short_card.properties["new_commit"] )
                           if type(temp) is not list:
                               temp = []
                       except:
                               temp = []
               
                       temp.append( text)
 
  
                       if error_flag == True:
                           short_card.properties["label"] = "red"
                       else:
                           short_card.properties["label"] = "green"

 
               
                       temp.append( text)
                       short_card.properties["new_commit"] = json.dumps(temp)
                       short_card.push()      
               


   
if __name__ == "__main__":

   redis_startup                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   slack_json = redis_startup.hget("MEDIA_DRIVERS","Slack")
   slack_dict   = json.loads( slack_json )
   token           = slack_dict["token"]
 
   slack = Slacker(token)

   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
   cc              = Capped_Collections( mongodb_db, mongodb_col, db_name = "Capped_Colections" ) 
   rc         = Rabbitmq_Remote_Connections()
   qc         = Query_Configuration()
   cd         = Update_Irrigation_Valve_Current_Draw(rc,qc,cc)
   av         = Analyize_Valve_Current_Data(qc)

   trello_json     = redis_startup.hget("MEDIA_DRIVERS","TRELLO")
   trello_dict     = json.loads( trello_json )
   tm         = Trello_Management( trello_dict )
   td              = Transfer_Data( tm, qc , slack,"#System-Issues" ) 

   #cd.update_coil_current(3)
   cd.update_coil_current(1)
   av.analyize_data()
   #td.update_cards()
   #print mongodb_col.collection_names("Capped_Colections")
   
