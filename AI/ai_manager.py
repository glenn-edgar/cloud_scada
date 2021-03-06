#
# File: ai_manager.py
#
#
#
#
#
import pika
import uuid
import json
import base64
import datetime
import time
import string
import urllib2
import math
import redis
import json
import py_cf 
import os
import math
from media_drivers.trello_management      import Trello_Management 
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
from remote_statistics                    import Analyize_Controller_Parameters
from remote_statistics                    import Analyize_Remote_Connectivity
from populate_trello_data_base            import Transfer_Data
from manage_event_queue                   import Monitor_Event_Queues
from slacker                              import Slacker



if __name__ == "__main__":
   redis_startup                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   slack_json = redis_startup.hget("MEDIA_DRIVERS","Slack")
   slack_dict   = json.loads( slack_json )
   token           = slack_dict["token"]
   
   slack = Slacker(token)
 
   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
   trello_json     = redis_startup.hget("MEDIA_DRIVERS","TRELLO")
   trello_dict     = json.loads( trello_json )
   tm              = Trello_Management( trello_dict )
   qc              = Query_Configuration()
   td              = Transfer_Data( tm, qc , slack,"#System-Issues" ) 
   rc              = Rabbitmq_Remote_Connections()

   cc              = Capped_Collections( mongodb_db, mongodb_col, db_name = "Capped_Colections" ) 
   idd             = Update_Irrigation_Data(rc,qc,cc) 
   cd              = Update_Irrigation_Valve_Current_Draw(rc,qc,cc)
   aid             = Analyize_Valve_Current_Data( qc )
   ac              = Analyize_Controller_Parameters( rc, qc )
   ar              = Analyize_Remote_Connectivity(rc, qc)
   cf              = py_cf.CF_Interpreter()
   me              = Monitor_Event_Queues( rc, qc, cf )
   av              = Analyize_Valve_Current_Data(qc)

   cf.define_chain("Initialize_System",True)
   cf.insert_link( "link_1","Enable_Chain",[["Check_Online","Update_Trello","Monitor_Events","End_Of_Day_House_Keeping"]])
   cf.insert_link( "link_2","One_Step",[ac.clear_ping] )
   cf.insert_link( "link_3","One_Step",[ac.clear_controller_resets] )
   cf.insert_link( "link_4","Disable_Chain",[["Initialize_System"]])
   


   cf.define_chain("Check_Online",False)
   cf.insert_link( "link_1","One_Step",[ac.ping_controllers] )
   cf.insert_link( "link_1","WaitTime",[5*60,0 ])
   cf.insert_link( "link_2","Reset",[] )
     
   cf.define_chain("Monitor_Events",False)
   cf.insert_link( "link_2","One_Step",[me.monitor_event_queue_cf] )
   cf.insert_link( "link_1","WaitTime",[15*60,0 ])
   cf.insert_link( "link_2","Reset",[] )



   cf.define_chain("Update_Trello",False )
   cf.insert_link( "link_1","WaitTime",[4*3600,0] )
   cf.insert_link( "link_2","One_Step",[ac.update_controller_properties] )
   cf.insert_link( "link_3","One_Step",[ar.analyize_data] )
   cf.insert_link( "link_3",  "One_Step",[idd.update_irrigation_data_cf] )
   cf.insert_link( "link_2",  "One_Step",[cd.update_coil_current_cf] )
   cf.insert_link( "link_3",  "One_Step", [av.analyize_data])
   cf.insert_link( "link_4","One_Step",[ td.update_cards ] )
   cf.insert_link( "link_5","Reset",[])
 
   
 
   cf.define_chain("End_Of_Day_House_Keeping",False)
   cf.insert_link( "link_0",   "WaitTod", ["*","8","*","*"]) # GMT Time add +8 to time  or +9 in summer
   cf.insert_link( "link_1",  "Log",      ["made it here"])
   cf.insert_link( "link_2",  "One_Step", [ac.clear_controller_resets ] )
   cf.insert_link( "link_3",  "One_Step", [ac.clear_ping] )
   cf.insert_link( "link_5",  "WaitTod",  ["*",9,"*","*" ] ) # GMT Time add +8 to time  or +9 in summer
   cf.insert_link( "link_6",  "Reset",    [] )

   cf_environ = py_cf.Execute_Cf_Environment( cf )
   cf_environ.execute()

