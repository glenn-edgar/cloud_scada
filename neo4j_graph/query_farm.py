
class Query_Farm():

   def __init__( self, qc ):
       self.qc = qc #qc is the class Query_Configuration defined in grap_functions

   def find_systems( self ): 
       return self.qc.match_labels("SYSTEM") 
   
   def find_sites( self ):
       return self.qc.match_labels( "SITE")

   def find_controllers( self ):
       return self.qc.match_labels( "CONTROLLER")

   def find_alert_queues( self):
        return self.qc.match_labels( "ACTIVE_ALARMS")

   def find_event_queues( self):
         return self.qc.match_labels( "EVENT_QUEUE")

   def find_system_alert_queues( self ):
        return self.qc.match_labels( "ACTIVE_ALARMS")

   def find_system_event_queues( self):
         return self.qc.match_labels( "SYSTEM_EVENT_QUEUE")

   def find_schedules( self ):
       return self.qc.match_labels( "SCHEDULE")


   def find_flow_sensors( self ):
        return self.qc.match_labels( "FLOW_SENSOR")

   def find_remote_servers( self ):
       return self.qc.match_labels( "UDP_IO_SERVER")

   def find_modbus_rtu_serial_interface( self ):
       return self.qc.match_labels( "RTU_INTERFACE")

   def find_rtu_units( self ):
       return self.qc.match_labels( "REMOTE")  

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
       

