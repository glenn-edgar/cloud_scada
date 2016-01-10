#
#  Construct Readonly Aspects of Document Data Base
#
#
#
#
#
import copy
import json
import pymongo
from pymongo import MongoClient

from configure_system import Query_Configuration
from mongodb.collection_functions import *

class Name_Space_Decoder():

   def __init__( self,key_list = [ None,"system","site","controller"] ):
       self.top_key_list = key_list

   def decode_name_space( self, app_key_list, name_space, node ):
       key_list = copy.copy(self.top_key_list)
       #print "key_list",key_list
       key_list.extend(app_key_list)
       #print key_list,app_key_list
       name_space_list = node.properties["workspace_name"].split("/" )
       #print "key_list",key_list,name_space_list
       if len(key_list) != len(name_space_list):
            print "unequal name space lengths"
            raise 
       
       for i in range(0, len(key_list)):
           if key_list[i] != None:
               node.properties[key_list[i]] = name_space_list[i]
       


class Fectch_Graph_Objects():         
    
   def __init__( self ):
       self.decoder = Name_Space_Decoder()

   def fetch_objects( self,name_space, relationship,db_name,collection_name,helper_function=None ):
      objects = qc.find_matching_nodes( relationship )
      #print "objects",objects
      for i in objects:
         #print "i",i.properties
         self.decoder.decode_name_space(name_space,name_space,i )
         mongodb_col.remove_document( db_name, collection_name, {"workspace_name":i.properties["workspace_name"]} )
         #print mongodb_col.collection_number(db_name, collection_name)
         mongodb_col.insert_document( db_name, collection_name,[json.loads(json.dumps(i.properties))])
         print mongodb_col.collection_number(db_name, collection_name)
         if helper_function != None:
            helper_function(i)


if __name__ == "__main__" :

   def add_schedule_steps( node ):
       number = node.properties["number"]
       ref_wrksp = node.properties["workspace_name"]
       for i in range(0,number):
          
          workspace_name = ref_wrksp+"/IRRIGATiON/"+str(i)
          if mongodb_col.collection_exits( "irrigation_collections", workspace_name  ) == False:
              mongodb_col.create_collection( "irrigation_collections", workspace_name, capped=True, max_number=256,collection_size=1000000 )
          else:
             print "irrigation collection not created"

          workspace_name = ref_wrksp+"/COIL/"+str(i)
          if mongodb_col.collection_exits( "irrigation_collections", workspace_name  ) == False:
              mongodb_col.create_collection( "irrigation_collections", workspace_name, capped=True, max_number=256,collection_size=1000000 )
          else:
             print "irrigation collection not created"


   def add_alarm_event_queues( node ):
       ref_wrksp = node.properties["workspace_name"]
       queues = ["alarm_queue","event_queue","system_alarm_queue","system_event_queue" ]
       for i in queues:
           workspace_name = ref_wrksp+"/"+i
           if mongodb_col.collection_exits( i, workspace_name  ) == False:
               mongodb_col.create_collection( i, workspace_name, capped=True, max_number=256,collection_size=1000000 )
           else:
             print "queue  "+ref_wrksp+"   "+i+"  not created"



   qc = Query_Configuration()
   fgc = Fectch_Graph_Objects()
   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
#
# Create Data Bases
#
#
   mongodb_db.create_database( "index")  # readonly properties of graph nodes
   mongodb_db.create_database( "runtime") # writeable properties of graph nodes 
   mongodb_db.create_database( "alarm_queue")
   mongodb_db.create_database( "event_queue")         #capped collection
   mongodb_db.create_database( "system_alarm_queue")    
   mongodb_db.create_database( "system_event_queue")  #capped collection
   mongodb_db.create_database( "irrigation_collections") #capped collection

#  
#  create collections in db index
#
#
   mongodb_col.create_collection( "index","controllers", capped=False )
   mongodb_col.create_collection( "index","schedules", capped=False)
   mongodb_col.create_collection( "index","flow_sensors", capped=False)  
   mongodb_col.create_collection( "index","io_servers", capped=False)
   mongodb_col.create_collection( "index","rtu_interfaces", capped=False)   
   mongodb_col.create_collection( "index","remotes", capped=False)


#
#  create collections in db runtime
#
#
   mongodb_col.create_collection( "runtime","controllers", capped=False )
   mongodb_col.create_collection( "runtime","remotes", capped=False)


# process controllers
   fgc.fetch_objects( [],"CONTROLLER","index","controllers",add_alarm_event_queues )
  

# process schedules
   fgc.fetch_objects( ["schedule"],"SCHEDULE","index","schedules",add_schedule_steps )
# process flow meters
   fgc.fetch_objects( ["flow_sensors"],"FLOW_SENSOR","index","flow_sensors" )
# process udp servers
   fgc.fetch_objects( ["io_servers"],"UDP_IO_SERVER","index","io_servers" )
# process rtu interfaces
   fgc.fetch_objects( ["io_servers","rtu_interfaces"],"RTU_INTERFACE","index","rtu_interfaces")
# process remote units
   fgc.fetch_objects( ["io_servers","rtu_interfaces","remotes"],"REMOTE","index","remotes" )  
   names = mongodb_col.collection_names( "irrigation_collections" )

