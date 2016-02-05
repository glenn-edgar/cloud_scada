class Monitor_Irrigation_Queue():


   def __init__( self, rabbitmq_remote_connections,query_configuration ):
       self.rc = rabbitmq_remote_connections
       self.qc = query_configuration
       self.cc_max_number   = 512
       self.cc_db_size      = 5000000

 
     

   def monitor_queue( self , cloud_queue,  remote_queue ):
       controller_list = qc.match_labels("CONTROLLER")
       for i in controller_list:
           vhost = i.properties["vhost"]
           # look for local queue post exception if not found
           # local mongodb queue
           # find number in queue # if 0 return

           # for number in queue
           #    execute rpop
           #    store data in queue
           #    execute any action -- store in mongodb 
           

class Derived_Handlers:

   def __init__( self ):
       self.dm = {} # derived processing
       self.dm["RESISTANCE_CHECK"]            = { "name": "RESISTANCE_CHECK",             "handler": self.resistance_check   }
       self.dm["CLEAN_FILTER"]                = { "name": "CLEAN_FILTER",                 "handler": self.clean_filter       }
       self.dm["irrigatation_store_object"]   = { "name": "irrigatation_store_object",    "handler": self.update_irrigation  }
       self.dm["CHECK_OFF"]                   = { "name": "CHECK_OFF",                    "handler":self.check_off           }
       self.am = {} # alert messages
       self.am["IRRIGATION:FLOW_ABORT"]       = { "name": "IRRIGATION:FLOW_ABORT",        "handler": self.operation_errors }
       self.am["IRRIGATION:CURRENT_ABORT"]    = { "name": "IRRIGATION:CURRENT_ABORT",     "handler": self.operation_errors }
       self.am["REBOOT"]                      = { "name": "REBOOT",                       "handler": self.system_errors }
       self.am["RESTART"]                     = { "name": "RESTART",                      "handler": self.system_errors }
       self.am["Bad Irrigation Command"]      = { "name": "Bad Irrigation Command",       "handler": self.system_errors }
       self.am["non_existant_schedule"]       = { "name": "non_existant_schedule",        "handler": self.system_errors }
       self.st = {} # status messages
       self.st["IRRIGATION:START:ETO_RESTRICTION"]  = { "name": "IRRIGATION:START:ETO_RESTRICTION", "handler": self.operational_update}
       self.st["SUSPEND_OPERATION"]                 = { "name": "SUSPEND_OPERATION",                "handler": self.operational_update }
       self.st["SKIP_STATION"]                      = { "name": "SKIP_STATION",                     "handler": self.operational_update }
       self.st["DIAGNOSTICS_SCHEDULE_STEP_TIME"]    = { "name": "DIAGNOSTICS_SCHEDULE_STEP_TIME",   "handler": self.operational_update }
       self.st["OPEN_MASTER_VALVE"]                 = { "name": "OPEN_MASTER_VALVE",                "handler": self.operational_update }
       self.st["RESUME_OPERATION"]                  = { "name": "RESUME_OPERATION",                 "handler": self.operational_update }
         

   def register_controllers( self, controller_node ):
       # 
       # Get queue nodes and store them in self object
       #       
       pass  


   def process( self, controller_node, json_object ):
       pass

   def resistance_check( self, json_object ):
       pass

   def clean_filter( self, json_object ):
       pass

   def update_irrigation( self,json_object):
       pass

class Alert_Managers:
  
   def __init__( self ):
   



#YELLOW  operator actions
"IRRIGATION:START:ETO_RESTRICTION","YELLOW", json_object  ) 
"SUSPEND_OPERATION","YELLOW"  )
"SKIP_STATION","YELLOW" ,{"skip: on"} )
"DIAGNOSTICS_SCHEDULE_STEP_TIME","YELLOW" , {"schedule_name":self.schedule_name, "schedule_step":self.schedule_step,"schedule_time":self.schedule_step_time})
"DIRECT_VALVE_CONTROL","YELLOW" ,{"remote":remote,"pin":pin,"time":schedule_step_time }) 
"OPEN_MASTER_VALVE","YELLOW" )
self.alarm_queue.store_past_action_queue("RESUME_OPERATION","GREEN"  )




                       
if __name__ == "__main__":

   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
 
   rc          = Rabbitmq_Remote_Connections()
   qc          = Query_Configuration()
   cc          = Capped_Collections( mongodb_db, mongodb_col, db_name = "Capped_Colections" ) 
   idd         = Update_Irrigation_Data(rc,qc)
   idd.update_irrigation_data(0)
                








'''
irrigation_ctrl_startup.py:              self.alarm_queue.store_past_action_queue("IRRIGATION:FLOW_ABORT","RED", { "schedule_name":json_object["schedule_name"],"step_number":json_object["step"],
irrigation_ctrl_startup.py:           self.alarm_queue.store_past_action_queue("IRRIGATION:CURRENT_ABORT","RED", { "schedule_name":json_object["schedule_name"],"step_number":json_object["step"] } )
irrigation_ctrl_startup.py:           self.alarm_queue.store_past_action_queue("IRRIGATION:START:ETO_RESTRICTION","YELLOW", json_object  ) 
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("IRRIGATION:END","GREEN", { "schedule_name":obj["schedule_name"],"step_name":obj["step"] } )
irrigation_ctrl_startup.py:                      self.alarm_queue.store_past_action_queue("Bad Irrigation Command","RED",object_data  )
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("SUSPEND_OPERATION","YELLOW"  )
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("RESUME_OPERATION","GREEN"  )
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("SKIP_STATION","YELLOW" ,{"skip: on"} )
irrigation_ctrl_startup.py:        alarm_queue.store_past_action_queue( "RESISTANCE_CHECK", "GREEN",  { "action":"start" } )        
irrigation_ctrl_startup.py:        alarm_queue.store_past_action_queue( "CHECK_OFF", "GREEN",  { "action":"start" } )        
irrigation_ctrl_startup.py:        alarm_queue.store_past_action_queue( "CLEAN_FILTER", "GREEN",  { "action":"start" } )        
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("OFFLINE","RED"  )
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("QUEUE_SCHEDULE","GREEN",{ "schedule":self.schedule_name } ) 
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("QUEUE_SCHEDULE_STEP","GREEN",{ "schedule":self.schedule_name,"step":self.schedule_step } )
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("DIAGNOSTICS_SCHEDULE_STEP_TIME","YELLOW" , {"schedule_name":self.schedule_name, "schedule_step":self.schedule_step,"schedule_time":self.schedule_step_time})
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("DIRECT_VALVE_CONTROL","YELLOW" ,{"remote":remote,"pin":pin,"time":schedule_step_time }) 
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("OPEN_MASTER_VALVE","YELLOW" )
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("CLOSE_MASTER_VALVE","GREEN"  )
irrigation_ctrl_startup.py:      self.alarm_queue.store_past_action_queue("REBOOT","RED"  )
irrigation_ctrl_startup.py:       self.alarm_queue.store_past_action_queue("RESTART","RED"  )
irrigation_ctrl_startup.py:        self.alarm_queue.store_past_action_queue("CLEAN_FILTER","GREEN"  )
irrigation_ctrl_startup.py:           alarm_queue.store_past_action_queue( "CHECK_OFF", "RED",  { "action":"bad","flow_rate":temp } )           
irrigation_ctrl_startup.py:           alarm_queue.store_past_action_queue( "CHECK_OFF", "GREEN",  { "action":"good","flow_rate":temp } )
irrigation_ctrl_startup.py:       #alarm_queue.store_past_action_queue("START_UP","RED"  )
irrigation_ctrl_startup.py:       #alarm_queue.store_past_action_queue("START_UP","RED"  )
watch_dog.py:   def store_alarm_queue( self, event, data ):
watch_dog.py:                     self.store_alarm_queue( "wd_kill_process", {"index":i, "description": description} )
watch_dog.py:            self.store_alarm_queue( "wd_exception",{"index":i} )
watch_dog.py:  wd.store_alarm_queue( "wd_startup",None )
"QUEUES:CLOUD_ALARM_QUEUE"

  def __init__(self,redis_server, alarm_queue = "QUEUES:CLOUD_ALARM_QUEUE", action_queue = "QUEUES:SPRINKLER:PAST_ACTIONS"):
       self.redis = redis_server
       self.alarm_queue = alarm_queue
       self.action_queue = action_queue

   def store_past_action_queue( self, event, status ,data = None):
       log_data = {}
       log_data["event"]   = event
       log_data["data"]     = data
       log_data["time" ]    = time.time()
       log_data["status"]   = status
       json_data            = json.dumps(log_data)
       json_data            =  base64.b64encode( json_data )
       self.redis.lpush( self.action_queue , json_data)
       self.redis.ltrim( self.action_queue ,0, 120 )
       self.store_alarm_queue( event,status, data )


   def store_alarm_queue( self, event,status, data ):
       log_data = {}
       log_data["event"] = event
       log_data["data"]     = data
       log_data["time" ]    = time.time()
       log_data["status"]   = status
       json_data            = json.dumps(log_data)
       json_data            =  base64.b64encode( json_data )
       self.redis.lpush( self.alarm_queue , json_data)
       self.redis.ltrim( self.alarm_queue ,0, 1000 )

   def store_event_queue( self, event, data ):
       log_data = {}
       log_data["event"] = event
       log_data["status"] = "INFO"
       log_data["data"]  = data
       log_data["time"]  = time.time()
       json_data = json.dumps(log_data)
       json_data = base64.b64encode(json_data)
       self.redis.lpush( self.alarm_queue, json_data)
       self.redis.ltrim( self.alarm_queue, 0,800)
  

external_control.py:        self.cloud_queue.store_event_queue( "store_eto", eto_data,status = "GREEN") 
irrigation_ctrl_startup.py:       self.alarm_queue.store_event_queue( "start_step", { "schedule":schedule_name, "step":step } )
irrigation_ctrl_startup.py:       self.alarm_queue.store_event_queue( "irrigatation_store_object", obj )
irrigation_ctrl_startup.py:           self.store_event_queue( "irrigation_schedule_start", json_object )
irrigation_ctrl_startup.py:           self.store_event_queue( "irrigation_schedule_stop", json_object )
irrigation_ctrl_startup.py:           self.store_event_queue( "non_existant_schedule", json_object )
watch_dog.py:      cloud_queue.store_event_queue(  event, data,status ="RED" )
'''
