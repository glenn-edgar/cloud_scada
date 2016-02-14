import copy
import json
import redis
from neo4j_graph.graph_functions         import Query_Configuration
from rabbit.rabbitmq_client              import RabbitMq_Client
from rabbit.client_commands              import Status_Alert_Cmds        
from configure_rabbit_queues             import Rabbitmq_Remote_Connections
from mongodb.collection_functions import Mongodb_Collection
from mongodb.collection_functions import Mongodb_DataBase
from pymongo import MongoClient
from mongodb_capped_collections import Capped_Collections


class Analyize_Controller_Parameters():

   def __init__( self, rabbitmq_remote_connections, query_configuration ):
       self.rc = rabbitmq_remote_connections
       self.qc = query_configuration
 

   def update_properties( self, card, limits, title,value ):
       value = float( value)
       if value < limits["yellow"]:
           color = "green"
           text = "Title: "+str(title)+" Value: "+str(value) 
       elif value < limits["red"]:
           color = "yellow"
           text = "**Error Title: "+str(title)+" Value: "+str(value)+" **"  
       else:
           color = "red"
           text = "**Error Title: "+str(title)+" Value: "+str(value)+" **"  
       try:
           temp = json.loads( card.properties["new_commit"] )
           if type(temp) is not list:
             temp = []
       except:
                  temp = []
       temp.append( text)
       card.properties["new_commit"] = json.dumps(temp)
       card.properties["label"] = color
       card.push()
       
 
   def clear_ping( self ):
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
           i.properties["ping_loss"] = 0
           i.properties["ping_count"] = 1
           i.push()



   def ping_controllers( self ):
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
           station_control = self.rc.get_station_control(  i.properties["vhost"] )
           ping_result = station_control.ping()
           if ping_result[0] == True:
               i.properties["ping_count"] = float(i.properties["ping_count"] )+1
           else:
               i.properties["ping_count"] = float(i.properties["ping_count"] )+1
               i.properties["ping_loss"] = float(i.properties["ping_loss"] )+1
           i.push()
           
    

   def update_controller_properties( self ):
       limits = {}
       limits["temperature"] = { "yellow":120, "red":140 }
       limits["irrigation_resets"] = {"yellow":1,"red":2 }
       limits["system_resets"] = {"yellow":1,"red":2 }
       limits["ping"]          = {"yellow":1,"red":10 }
       resets = {}
       resets["temperature"] = False
       resets["irrigation_resets"] = True
       resets["system_resets"] = True

       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
           card_dict   = json.loads( i.properties["card_dict"] )
           try:
              ping_loss = float(i.properties["ping_loss"])
           except:
              ping_loss = 0
           try:
              ping_counts = float(i.properties["ping_counts"])
           except:
              ping_counts = 1
           if ping_counts < 1:
              ping_counts = 1
           i.properties["ping_loss"] = 0
           i.properties["ping_count"] = 1
           i.push()
           rate = ping_loss/ping_counts
           card_name = card_dict["ping"]
           card = self.qc.match_relation_property_specific( "CONTROLLER","name",i.properties["name"],"DIAGNOSTIC_CARD","name",card_name )[0] 
           self.update_properties( card, limits["ping"],"RabbitMQ Packet Loss Rate", rate )
           rpc_queue = i.properties["rpc_queue"]
           redis_key = i.properties["redis_key"]
           station_control = self.rc.get_station_control(  i.properties["vhost"] )
           redis_data  = station_control.redis_hget_all( [ {"hash":redis_key} ] )[1][0]["data"]
           
           for j in redis_data.keys():
               if card_dict.has_key(j) == True:
                   card_name = card_dict[j]
                   card = self.qc.match_relation_property_specific( "CONTROLLER","name",i.properties["name"],"DIAGNOSTIC_CARD","name",card_name )[0]
                   self.update_properties( card, limits[j],j,redis_data[j] )
                   if resets[j] == True:
                       station_control.redis_hset( [{"hash":redis_key ,"key":j,"value":0 }] ) 
      
                  
                    










class Analyize_Remote_Connectivity:

   def __init__( self, query_control ):
       self.qc = query_control

   def convert_to_int( self, data ):
      
       return_value = {}
       print data, type(data)
       for i in data.keys():
           return_value[i] = int(data[i])
       return return_value

      
   def transform_data( self, connectivity ):
       
       data = self.convert_to_int( connectivity )
       error_rate = float(data["failures"])/float(data["counts"])
       message_loss = data["total_failures"]
       return error_rate, message_loss


   def analyize_data( self ):
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
           remote_list = self.qc.match_relation_property( "CONTROLLER","name", i.properties["name"],"REMOTE" )
           for j in remote_list:
               print j.properties
               connectivity  = json.loads( j.properties["connectivity"])
               error_rate, message_loss = self.transform_data( connectivity )
               card_dict   = json.loads( j.properties["card_dict"] )
               card                     = self.qc.match_relation_property_specific( "CONTROLLER","name",i.properties["name"],"DIAGNOSTIC_CARD","name",card_dict["connectivity"] )[0]
               
               text = "Controller: "+i.properties["name"]+ " Remote: "+j.properties["name"]+"\n"
               if (error_rate < .03 ) and ( message_loss == 0 ):
                    text = text+"Error Rate: "+str(error_rate)+" Message Loss: "+str(message_loss) 
                    color = "green"
               elif message_loss == 0:
                    text = text+"**Error Rate: "+str(error_rate)+" Message Loss: "+str(message_loss)+ " ERROR **" 
                    color = "yellow"
               else:
                    text = text+"**Error Rate: "+str(error_rate)+" Message Loss: "+str(message_loss)+ " ERROR **" 
                    color = "red"

               try:
                  temp = json.loads( card.properties["new_commit"] )
                  length = len(temp)
               except:
                  temp = []

               temp.append( text)
               print text, color
               card.properties["new_commit"] = json.dumps(temp)
               card.properties["label"] = color
               card.push()
      
               


   
if __name__ == "__main__":

   
   rc         = Rabbitmq_Remote_Connections()
   qc         = Query_Configuration()
   ac         = Analyize_Controller_Parameters( rc, qc )
   ar         = Analyize_Remote_Connectivity(qc)
  
   for i in range(0,10):
       ac.ping_controllers()
   ac.update_controller_properties()
   ac.clear_ping()
 


  
   #ar.analyize_data()
