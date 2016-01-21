
from graph_functions import Build_Configuration

class Construct_Farm():

   def __init__( self, bc):
      self.bc = bc # Build configuration in graph_functions

   def construct_system( self,name=None):
       self.bc.construct_node( True,"SYSTEM","SYSTEM",name,{} )
       
   def end_system( self):
       self.bc.pop_workspace()

   def construct_site( self,name=None,wired=True,address=None):
       self.bc.construct_node(  push_workspace=True,relationship="SITE", label="SITE", name=name, 
               properties ={"wired":wired,"address":address})

   def end_site( self ):
      self.bc.pop_workspace()

   def construct_controller( self,name,web_queue,rpc_queue,local_ip,controller_type,vhost):
       self.bc.construct_node(  push_workspace=True,relationship="CONTROLLER", label="CONTROLLER", name=name, 
               properties ={"web_queue":web_queue, "rpc_queue":rpc_queue,"local_ip":local_ip,"controller_type":controller_type,"vhost":vhost})


   def end_controller( self ):
       self.bc.pop_workspace()


   def add_event_queue( self, *args):
       self.bc.construct_node(  push_workspace=False,relationship="EVENT_QUEUE", label="EVENT_QUEUE", name="EVENT_QUEUE",
                                    properties = {} )


   def add_system_event_queue( self, *args):
       self.bc.construct_node(  push_workspace=False,
                                    relationship="SYSTEM_EVENT_QUEUE", 
                                    label="SYSTEM_EVENT_QUEUE", 
                                    name="SYSTEM_EVENT_QUEUE",
                                    properties = {} )



   def add_schedule( self,name,number):
       schedule_node = self.bc.construct_node(  push_workspace=True,relationship="SCHEDULE", label="IRRIGATION_SCHEDULE", name=name, 
                       properties ={"number":number})
       for i in range(0,number):
           self.bc.construct_node(  push_workspace=False,relationship="IRRIGATION_FLOW_STEP", label="IRRIGATION_FLOW_STEP", name= "FLOW/"+str(i+1), 
                       properties ={ })
           self.bc.construct_node(  push_workspace=False,relationship="IRRIGATION_COIL_STEP", label="IRRIGATION_COIL_STEP", name= "COIL/"+str(i+1), 
                       properties ={ })
           self.bc.construct_node(  push_workspace=False,relationship="IRRIGATION_FLOW_STEP_LIMIT", label="IRRIGATION_FLOW_STEP_LIMIT", name= "FLOW/"+str(i+1), 
                       properties ={ })
           self.bc.construct_node(  push_workspace=False,relationship="IRRIGATION_COIL_STEP_LIMIT", label="IRRIGATION_COIL_STEP_LIMIT", name= "COIL/"+str(i+1), 
                       properties ={ })

       self.bc.pop_workspace()


   def add_flow_sensor( self,name,controller,io,conversion_factor):
       return self.bc.construct_node(  push_workspace=False,relationship="FLOW_SENSOR", label="FLOW_SENSOR", name=name, 
               properties ={"name":name,"controller":controller,"io":io,"conversion_factor":conversion_factor})



   def add_udp_io_sever(self, name, ip,remote_type, port ):
       return self.bc.construct_node(  push_workspace=True,relationship="UDP_IO_SERVER", label="UDP_IO_SERVER", name=name, 
               properties ={"name":name,"ip":ip,"remote_type":remote_type,"port":port })


   def end_udp_io_server(self ):
       self.bc.pop_workspace()


   def add_rtu_interface(self, name ,protocol,baud_rate ):
       return self.bc.construct_node(  push_workspace=True,relationship="RTU_INTERFACE", label="RTU_INTERFACE", name=name, 
               properties ={"name":name,"protocol":protocol,"baud_rate":baud_rate })


   def add_remote( self, name,modbus_address,irrigation_station_number):
       self.bc.construct_node(  push_workspace=True,relationship="REMOTE", label="REMOTE", name=name, 
               properties ={"name":name,"modbus_address":modbus_address,"irrigation_station_number":irrigation_station_number })
       for i in range(0,irrigation_station_number):
           self.bc.construct_node(  push_workspace=False,relationship="IRRIGATION_VALVE_CURRENT", label="IRRIGATION_VALVE_CURRENT", name = "VALVE_CURRENT/"+str(i+1), 
           properties ={ "active":False })           
           self.bc.construct_node(  push_workspace=False,relationship="IRRIGATION_VALVE_CURRENT_LIMIT", label="IRRIGATION_VALVE_CURRENT_LIMIT", name= "VALVE_CURRENT_LIMIT/"+str(i+1), 
           properties ={ "active":False  })
       self.bc.pop_workspace()


   def end_rtu_interface( self ):
       self.bc.pop_workspace()


class WorkSpace_Name_Decoder():

   
   def __init__( self,key_list = [ None,"system","site","controller"] ):
       self.top_key_list = key_list

   def decode_name_space( self, app_key_list, name_space, node ):
       key_list = copy.copy(self.top_key_list)
       key_list.extend(app_key_list)
       name_space_list = node.properties["workspace_name"].split("/" )
       if len(key_list) != len(name_space_list):
            print "unequal name space lengths"
            raise 
       
       for i in range(0, len(key_list)):
           if key_list[i] != None:
               node.properties[key_list[i]] = name_space_list[i]
       

