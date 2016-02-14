import json
import redis
import math
from neo4j_graph.graph_functions          import Query_Configuration
from rabbit.rabbitmq_client               import RabbitMq_Client
from rabbit.client_commands               import Status_Alert_Cmds        
from configure_rabbit_queues              import Rabbitmq_Remote_Connections
from mongodb.collection_functions         import Mongodb_Collection
from mongodb.collection_functions         import Mongodb_DataBase
from pymongo                              import MongoClient
from mongodb_capped_collections           import Capped_Collections
from update_irrigation_data               import Update_Irrigation_Data
from update_irrigation_valve_current_draw import Update_Irrigation_Valve_Current_Draw
from update_irrigation_valve_current_draw import Analyize_Valve_Current_Data

if __name__ == "__main__":

   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
 
   rc          = Rabbitmq_Remote_Connections()
   qc          = Query_Configuration()
   cc          = Capped_Collections( mongodb_db, mongodb_col, db_name = "Capped_Colections" ) 
   idd         = Update_Irrigation_Data(rc,qc,cc) 
   cd          = Update_Irrigation_Valve_Current_Draw(rc,qc,cc)
   aid         = Analyize_Valve_Current_Data( qc )


   cd.update_coil_current(0)

   idd.update_irrigation_data(0)
 
   aid.analyize_data()
