
from graph_functions import Build_Configuration

class Construct_Farm():

   def __init__( self, bc):
      self.bc = bc # Build configuration in graph_functions

   def construct_system( self,name=None):
       self.bc.construct_node( True,"SYSTEM","SYSTEM",name,{} )
       
   def end_system( self):
       self.bc.pop_namespace()

   def construct_site( self,name=None,wired=True,address=None):
       self.bc.construct_node(  push_namespace=True,relationship="SITE", label="SITE", name=name, 
               properties ={"wired":wired,"address":address})

   def end_site( self ):
      self.bc.pop_namespace()

   def construct_controller( self,name,web_queue,rpc_queue,local_ip,controller_type,vhost):
       self.bc.construct_node(  push_namespace=True,relationship="CONTROLLER", label="CONTROLLER", name=name, 
               properties ={"web_queue":web_queue, "rpc_queue":rpc_queue,"local_ip":local_ip,"controller_type":controller_type,"vhost":vhost})


   def end_controller( self ):
       self.bc.pop_namespace()


   def add_event_queue( self, *args):
       self.bc.construct_node(  push_namespace=False,relationship="EVENT_QUEUE", label="EVENT_QUEUE", name="EVENT_QUEUE",
                                    properties = {} )


   def add_system_event_queue( self, *args):
       self.bc.construct_node(  push_namespace=False,
                                    relationship="SYSTEM_EVENT_QUEUE", 
                                    label="SYSTEM_EVENT_QUEUE", 
                                    name="SYSTEM_EVENT_QUEUE",
                                    properties = {} )


   def add_diagnostic_card_header( self, *args):
       self.bc.construct_node(  push_namespace=True,
                                    relationship="DIAGNOSTIC_CARD_HEADER", 
                                    label="DIAGNOSTIC_CARD_HEADER", 
                                    name="DIAGNOSTIC_CARD_HEADER",
                                    properties = { "organization": "lacimaoperations" } )

   def end_diagnostic_card_header( self, *args):
       self.bc.pop_namespace()  

   def add_diagnostic_card( self, name, board, list_group ):
       self.bc.construct_node(  push_namespace=False,
                                relationship="DIAGNOSTIC_CARD", 
                                label="DIAGNOSTIC_CARD", 
                                name = name,
                                 properties = { "board":board, "list_group":list_group } )

   def add_schedule_header( self ):
       return self.bc.construct_node(  push_namespace=True,relationship="Schedule_Header", label="Schedule_Header", name="Schedule_Header", 
               properties ={})

   def end_schedule_header( self ):
       self.bc.pop_namespace()   




   def add_schedule( self,name,number,flow_sensor_names):
       schedule_node = self.bc.construct_node(  push_namespace=True,relationship="IRRIGATION_SCHEDULE", label="IRRIGATION_SCHEDULE", name=name, 
                       properties ={"number":number})
       for i in range(0,number):
           self.bc.construct_node(  push_namespace=True,relationship="STEP", label="STEP", name=str(i+1), 
                       properties ={"number":number})
           self.bc.construct_node(  push_namespace=True,relationship="FLOW_SENSOR_HEADERS", label="FLOW_SENSOR_HEADERS", name="FLOW_SENSOR_HEADERS", 
                       properties ={ })
           for j in flow_sensor_names:
               self.bc.construct_node( push_namespace = True,  relationship="FLOW_SENSOR_HEADER", label = "FLOW_SENSOR_HEADER", name = j, properties={} )
               self.bc.construct_node( push_namespace = False, relationship="FLOW_SENSOR_LIMIT", label = "FLOW_SENSOR_LIMIT", name = j, properties={} )
               self.bc.construct_node( push_namespace = False, relationship="FLOW_SENSOR_VALUE", label = "FLOW_SENSOR_VALUE", name = j, properties={} )
               self.bc.pop_namespace()
           self.bc.pop_namespace()
           self.bc.construct_node(  push_namespace=False,relationship="COIL_CURRENT", label="COIL_CURRENT", name= "COIL_CURRENT", 
                       properties ={ })
           self.bc.construct_node(  push_namespace=False,relationship="COIL_CURRENT_LIMIT", label="COIL_CURRENT_LIMIT", name= "COIL_CURRENT_LIMIT",
                       properties ={ })
           
       
           for j in flow_sensor_names:
               self.bc.construct_node( push_namespace = False, relationship="FLOW_SENSOR_LIMIT", label = "FLOW_SENSOR_LIMIT", name = j, properties={} )
           
           self.bc.pop_namespace()
  
       self.bc.pop_namespace()


   def add_flow_sensor_header( self ):
       return self.bc.construct_node(  push_namespace=True,relationship="FLOW_SENSOR_HEADER", label="FLOW_SENSOR_HEADER", name="flow_sensor_header", 
               properties ={})

   def end_flow_sensor_header( self ):
       self.bc.pop_namespace()   

   def add_flow_sensor( self,name,controller,io,conversion_factor):
       return self.bc.construct_node(  push_namespace=False,relationship="FLOW_SENSOR", label="FLOW_SENSOR", name=name, 
               properties ={"name":name,"controller":controller,"io":io,"conversion_factor":conversion_factor})

   

   def add_udp_io_sever(self, name, ip,remote_type, port ):
       return self.bc.construct_node(  push_namespace=True,relationship="UDP_IO_SERVER", label="UDP_IO_SERVER", name=name, 
               properties ={"name":name,"ip":ip,"remote_type":remote_type,"port":port })


   def end_udp_io_server(self ):
       self.bc.pop_namespace()


   def add_rtu_interface(self, name ,protocol,baud_rate ):
       return self.bc.construct_node(  push_namespace=True,relationship="RTU_INTERFACE", label="RTU_INTERFACE", name=name, 
               properties ={"name":name,"protocol":protocol,"baud_rate":baud_rate })


   def add_remote( self, name,modbus_address,irrigation_station_number):
       self.bc.construct_node(  push_namespace=True,relationship="REMOTE", label="REMOTE", name=name, 
               properties ={"name":name,"modbus_address":modbus_address,"irrigation_station_number":irrigation_station_number })
       self.bc.construct_node(  push_namespace=True,relationship="IRRIGATION_VALVE_CURRENT_HEADER", label="IRRIGATION_VALVE_CURRENT_HEADER", name = "valve_current_header", 
           properties ={ })           
       for i in range(0,irrigation_station_number):
           self.bc.construct_node(  push_namespace=False,relationship="IRRIGATION_VALVE_CURRENT", label="IRRIGATION_VALVE_CURRENT", name = str(i+1), 
           properties ={ "active":False })         
       self.bc.pop_namespace()

       self.bc.construct_node(  push_namespace=True,relationship="IRRIGATION_VALVE_CURRENT_HEADER", label="IRRIGATION_VALVE_CURRENT_LIMIT_HEADER", name = "valve_current_limit_header", 
           properties ={ })           
       for i in range(0,irrigation_station_number):  
           self.bc.construct_node(  push_namespace=False,relationship="IRRIGATION_VALVE_CURRENT_LIMIT", label="IRRIGATION_VALVE_CURRENT_LIMIT", name= str(i+1), 
           properties ={ "active":False  })
  
       self.bc.pop_namespace()
       self.bc.pop_namespace()


   def end_rtu_interface( self ):
       self.bc.pop_namespace()


class Namespace_Decoder():

   
   def __init__( self,key_list = [ None,"system","site","controller"] ):
       self.top_key_list = key_list

   def decode_name_space( self, app_key_list, name_space, node ):
       key_list = copy.copy(self.top_key_list)
       key_list.extend(app_key_list)
       name_space_list = node.properties["namespace"].split("/" )
       if len(key_list) != len(name_space_list):
            print "unequal name space lengths"
            raise 
       
       for i in range(0, len(key_list)):
           if key_list[i] != None:
               node.properties[key_list[i]] = name_space_list[i]
       

