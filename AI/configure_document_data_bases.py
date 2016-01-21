#
#  Construct Mongodb data bases and collections
#
#
#
#
#
import copy
import json
import pymongo
from pymongo import MongoClient


from mongodb.collection_functions import Mongodb_Collection
from mongodb.collection_functions import Mongodb_DataBase
from neo4j_graph.graph_functions  import Query_Configuration




if __name__ == "__main__" :

   def construct_capped_queues( label,database,max_number,collection_size  ):
       objects = qc.match_labels( label)
       
       for i in objects:
           
         
           if mongodb_col.collection_exits( database, i.properties["workspace_name"]  ) == False:
               mongodb_col.create_collection( database, i.properties["workspace_name"], capped=True, max_number= max_number, collection_size = collection_size )
           else:
               print i.properties["workspace_name"] + "  Queue not created"

      






   qc              = Query_Configuration()
   
   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
   mongodb_db.remove_all_databases()
# 
# Create Data Bases for capped collections
#
#
   mongodb_db.create_database( "DataQueues")       # history of events and queues capped collection
   max_number      = 1024
   collection_size = 10000000
   construct_capped_queues( "EVENT_QUEUE",           "DataQueues",  max_number,   collection_size  )
   construct_capped_queues( "SYSTEM_EVENT_QUEUE",    "DataQueues",  max_number,   collection_size  )
   construct_capped_queues( "IRRIGATION_FLOW_STEP",  "DataQueues",  max_number,   collection_size  )
   construct_capped_queues( "IRRIGATION_COIL_STEP",  "DataQueues",  max_number,   collection_size  )
   construct_capped_queues( "IRRIGATION_COIL_STEP",  "DataQueues",  max_number,   collection_size  )
   construct_capped_queues( "IRRIGATION_VALVE_CURRENT",  "DataQueues",  max_number,   collection_size  )
   print len( mongodb_col.collection_names( "DataQueues" ) )
   names = mongodb_col.collection_names("DataQueues")
   for i in range(0,len(names)):
       print i,names[i]


