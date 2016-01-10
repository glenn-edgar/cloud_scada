#
#  The purpose of this file is to load a system configuration
#  in the graphic data base
#



from py2neo import Graph, Node,Relationship
import copy

class Build_Configuration:
   def __init__( self):
      self.graph = Graph()
      self.graph.delete_all()
      self.workspace_name = ["Start"]
      self.workspace      = []

   def check_duplicates(self, label, name ):
       #print "label",label,name
       if self.graph.find_one(label ,property_key="name",property_value=name) != None:
            raise ValueError( "Duplicate Node",label,name )

   def get_workspace_name( self,name ):
       temp = copy.deepcopy(self.workspace_name) 
       temp.append(name)
       return_value = "/".join(temp )
       return return_value

   def get_workspace_node(self):
       return self.workspace[-1]       

   def pop_workspace( self ):
       del self.workspace_name[-1]
       del self.workspace[-1]         

   def construct_node(self, push_workspace,relationship, label, name, properties ):
       workspace_name = self.get_workspace_name(name)
       
       self.check_duplicates( label, name=workspace_name)
       
       node = Node(label,workspace_name)
       node.properties["workspace_name"]=workspace_name
       node.properties["name"] = name
       for i in properties.keys():
          node.properties[i]=properties[i]
       self.graph.create(node)
       if len(self.workspace) !=0:
           relation_enity = Relationship( self.get_workspace_node(),relationship,node) 
           
           self.graph.create( relation_enity )

       if push_workspace == True:
          self.workspace_name.append(name)
          self.workspace.append(node)
       
   def construct_system( self,name=None):
       self.construct_node( True,"SYSTEM","SYSTEM",name,{} )
       
   def end_system( self):
       self.pop_workspace()

   def construct_site( self,name=None,wired=True,address=None):
       return self.construct_node(  push_workspace=True,relationship="SITE", label="SITE", name=name, 
               properties ={"wired":wired,"address":address})

   def end_site( self ):
      self.pop_workspace()

   def construct_controller( self,name,web_queue,rpc_queue,local_ip,controller_type):
       return self.construct_node(  push_workspace=True,relationship="CONTROLLER", label="CONTROLLER", name=name, 
               properties ={"web_queue":web_queue, "rpc_queue":rpc_queue,"local_ip":local_ip,"controller_type":controller_type})


   def end_controller( self ):
       self.pop_workspace()

   def add_active_alarms(self,*args):
         return self.construct_node(  push_workspace=False,relationship="ACTIVE_ALARMS", label="ACTIVE_ALARMS", name="ACTIVE_ALARMS",
                                      properties = {} )

   def add_event_queue( self, *args):
       return self.construct_node(  push_workspace=False,relationship="EVENT_QUEUE", label="EVENT_QUEUE", name="EVENT_QUEUE",
                                    properties = {} )

   def add_system_active_alarms(self,*args):
         return self.construct_node(  push_workspace=False,
                                      relationship="SYSTEM_ACTIVE_ALARMS",
                                      label="SYSTEM_ACTIVE_ALARMS",                       
                                      name="SYSTEM_ACTIVE_ALARMS",
                                      properties = {} )

   def add_system_event_queue( self, *args):
       return self.construct_node(  push_workspace=False,
                                    relationship="SYSTEM_EVENT_QUEUE", 
                                    label="SYSTEM_EVENT_QUEUE", 
                                    name="SYSTEM_EVENT_QUEUE",
                                    properties = {} )



   def add_schedule( self,name,number):
       return self.construct_node(  push_workspace=False,relationship="SCHEDULE", label="SCHEDULE", name=name, 
               properties ={"name":name,"number":number})




   def add_flow_sensor( self,name,controller,io,conversion_factor):
       return self.construct_node(  push_workspace=False,relationship="FLOW_SENSOR", label="FLOW_SENSOR", name=name, 
               properties ={"name":name,"controller":controller,"io":io,"conversion_factor":conversion_factor})



   def add_udp_io_sever(self, name, ip,remote_type, port ):
       return self.construct_node(  push_workspace=True,relationship="UDP_IO_SERVER", label="UDP_IO_SERVER", name=name, 
               properties ={"name":name,"ip":ip,"remote_type":remote_type,"port":port })


   def end_udp_io_server(self ):
       self.pop_workspace()


   def add_rtu_interface(self, name ,protocol,baud_rate ):
       return self.construct_node(  push_workspace=True,relationship="RTU_INTERFACE", label="RTU_INTERFACE", name=name, 
               properties ={"name":name,"protocol":protocol,"baud_rate":baud_rate })


   def add_remote( self, name,modbus_address):
       return self.construct_node(  push_workspace=False,relationship="REMOTE", label="REMOTE", name=name, 
               properties ={"name":name,"modbus_address":modbus_address })


   def end_rtu_interface( self ):
       self.pop_workspace()

class Query_Configuration:

   def __init__( self, graph=None):
        if graph == None:
          self.graph = Graph()

      

   def match_labels( self, label):
       results =  self.graph.cypher.execute("MATCH (m:"+label+")  RETURN m")
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value



   def find_nodes( self, top_enity, relationship):
       query_string = "MATCH n-[:"+relationship+"]->m Where n.workspace_name = '"+top_enity.properties["workspace_name"]+"'  RETURN m"  
       #print "---------query string ---------------->"+query_string
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value
 


   def find_systems( self ):
       return self.match_labels("SYSTEM") 
   
   def find_sites( self, system_node ):
       return self.find_nodes( system_node,"SITE")

   def find_controllers( self, site_node ):
       return self.find_nodes( site_node,"CONTROLLER")

   def find_alert_queues( self, controller_node):
        return self.find_nodes( controller_node,"ACTIVE_ALARMS")

   def find_event_queues( self, controller_node):
         return self.find_nodes( controller_node,"EVENT_QUEUE")

   def find_system_alert_queues( self, controller_node):
        return self.find_nodes( controller_node,"ACTIVE_ALARMS")

   def find_system_event_queues( self, controller_node):
         return self.find_nodes( controller_node,"SYSTEM_EVENT_QUEUE")

   def find_schedules( self, controller_node ):
       return self.find_nodes( controller_node,"SCHEDULE")


   def find_flow_sensors( self, controller_node ):
        return self.find_nodes( controller_node,"FLOW_SENSOR")

   def find_remote_servers( self, controller ):
       return self.find_nodes( controller,"UDP_IO_SERVER")

   def find_modbus_rtu_serial_interface( self, remote_node ):
       return self.find_nodes( remote_node,"RTU_INTERFACE")

   def find_rtu_units( self, modbus_serial_interface ):
       return self.find_nodes( modbus_serial_interface,"REMOTE")  

   def find_matching_nodes( self, relationship ):
       query_string = "MATCH n-[:"+relationship+"]->m   RETURN m"  
       #print "---------query string ---------------->"+query_string
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value
 
if __name__ == "__main__" :
   bc = Build_Configuration()
   bc.construct_system("Onyx Operations")
   bc.construct_site( name="LaCima",wired=True,address="21005 Paseo Montana Murrieta, Ca 92562")
   bc.construct_controller( "PI_1","rpc_queue","alert_status_queue","192.168.1.82","irrigation/1")
   bc.add_active_alarms()
   bc.add_event_queue()
   bc.add_system_active_alarms()
   bc.add_system_event_queue()

   bc.add_schedule( name='fruit_tree_low_water',number=11)
   bc.add_schedule( name='flowers',number=14)
   bc.add_schedule( name='clean-filter',number=6)
   bc.add_schedule( name='house',number=5)

   bc.add_flow_sensor(name='main_sensor',controller='satellite_1',io=1,conversion_factor = 0.0224145939)

   bc.add_udp_io_sever(name="main_remote", ip="192.168.1.82",remote_type= "UDP", port=5005 )
   bc.add_rtu_interface(name = "rtu_2",protocol="modify_modbus",baud_rate=38400 )
   bc.add_remote(  name="satellite_1",modbus_address=100 )
   bc.add_remote(  name="satellite_2",modbus_address=125 )
   bc.add_remote(  name="satellite_3",modbus_address=170) 
   bc.end_rtu_interface()
   bc.end_udp_io_server()
   bc.end_controller()
   bc.end_site()
   bc.end_system()

   print bc.graph.order
   print bc.graph.node_labels
   print bc.graph.relationship_types


   qc = Query_Configuration()
   systems =  qc.find_systems()
   print systems[0].properties["name"]
   sites = qc.find_sites( systems[0] )
   print sites[0].properties["name"]
   controllers= qc.find_controllers(sites[0])
   print controllers[0].properties["name"]
   alert_queues = qc.find_alert_queues(controllers[0])
   print alert_queues[0].properties["name"]
   event_queues = qc.find_event_queues(controllers[0])
   print event_queues[0].properties["name"]
   system_alert_queues = qc.find_system_alert_queues(controllers[0])
   print alert_queues[0].properties["name"]
   system_event_queues = qc.find_system_event_queues(controllers[0])
   print system_event_queues[0].properties["name"]
   schedules = qc.find_schedules( controllers[0] )
   for i in schedules:
     print i.properties["name"],i.properties["number"]
   flow_sensors = qc.find_flow_sensors(controllers[0])
   print flow_sensors[0].properties["name"]
   remote_servers = qc.find_remote_servers(controllers[0])
   print remote_servers[0].properties["name"]
   interfaces = qc.find_modbus_rtu_serial_interface(remote_servers[0])
   print interfaces[0].properties["name"]
   remotes = qc.find_rtu_units(interfaces[0])
   for i in remotes:
     print i.properties["name"]
   print "mathing nodes test"
   schedules = qc.find_matching_nodes( "SCHEDULE" )
   for i in schedules:
     print i.properties["name"]

'''
class Query_Configuration:

   def __init__( self, graph):
      self.graph = graph
      

   def match_labels( self, label):
       results =  self.graph.cypher.execute("MATCH (m:"+label+")  RETURN m")
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value



   def find_nodes( self, top_enity, relationship):
       query_string = "MATCH n-[:"+relationship+"]->m Where n.name = '"+top_enity["name"]+"'  RETURN m"  
       #print "---------query string ---------------->"+query_string
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value
 
   def find_systems( self ):
       return self.match_labels("System")    

   def find_sites( self, system_node ):
       return self.find_nodes( system_node,"SITE")
     
  

   def find_controllers( self, site_node ):
       return self.find_nodes( site_node,"Site_Link")

   def find_alert_queues( self, controller_node):
        return self.find_nodes( system_node,"ALARM_QUEUE_LINK")

   def find_event_queues( self, controller_node):
         return self.find_nodes( system_node,"EVENT_QUEUE_LINK")

   def find_schedules( self, controller_node ):
       return self.find_nodes(  controller_node,"SCHEDULE_LINK")


   def find_flow_sensors( self, controller_node ):
       return self.find_nodes( controller_node,"FLOW_SENSOR")

   def find_remote_interfaces( self, site_node ):
       return self.find_nodes( site_node,"REMOTE_SERVER")

   def find_modbus_rtu_serial_interface( self, remote_node ):
       return self.find_nodes( remote_node,"MODBUS_RTU_SERIAL_LINK")

   def find_rtu_units( self, modbus_serial_interface ):
       return self.find_nodes( modbus_serial_interface,"MODBUS_RTU_NODE")  

       
if __name__ == "__main__" :
   bc = Build_Configuration("Onyx Operations")
   bc.construct_site(name="LaCima",wired=True,address="21005 Paseo Montana Murrieta Ca 92562")
   bc.construct_controller( name="LaCima", web_queue='rpc_queue', rpc_queue='alert_status_queue',local_ip="192.168.1.82")
   bc.construct_schedules( schedules=[{'name':'fruit_tree_low_water','number':11},{'name':'flowers','number':14}, {'name':'clean-filter','number':6},{'name':'house','number':5} ] )
   bc.construct_flow_sensors( flow_sensors=[{'name':'main_sensor','controller':'satellite_1',"pin":1,"conversion_factor":0.0224145939}, ] )
   bc.construct_remote_interface( name="main_remote", ip="192.168.1.82",remote_type= "UDP", port=5005)
   bc.construct_modbus_rtu_interface( name = "rtu_2",protocol="modify_modbus",baud_rate=38400 )
   bc.attach_modbus_rtu( [ {"name":"satellite_1","modbus_address":100 },{"name":"satellite_2","modbus_address":125 },{"name":"satellite_1","modbus_address":170 } ])
   print bc.graph.order
   print bc.graph.node_labels
   print bc.graph.relationship_types

   qc = Query_Configuration( bc.graph )
   systems                = qc.find_systems()
   print systems
 
   sites                  = qc.find_sites( systems[0] )
   print sites[0]
   controllers            = qc.find_controllers( sites[0] )
   print controllers[0]
   

   schedules              = qc.find_schedules( controllers[0] )
   print schedules
 
   sensors                = qc.find_flow_sensors( controllers[0] )
   print "sensors",sensors
   remote_interfaces      = qc.find_remote_interfaces( sites[0] )
   print "remote_interfaces",remote_interfaces
   modbus_rtu_interfaces   = qc.find_modbus_rtu_serial_interface( remote_interfaces[0] )
   print modbus_rtu_interfaces
   rtus                   = qc.find_rtu_units(  modbus_rtu_interfaces[0] )
   print rtus

'''
