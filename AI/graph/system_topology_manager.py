# the purpose of the system topology manager is to build a graph where
# data can be stored, analyized, and displayed


from py2neo import Graph, Node,Relationship

class Neo4j_Utilities:
    
   def __init__(self, graph):
      self.graph = graph



   def dump( self, node):
     for i in node:
           print i


class Node_Constructor:

   def __init__(self,*args):
      self.graph = Graph()
      self.graph.delete_all()
      system = Node("System")
      self.graph.create(system)
      self.current_system = system
      
   def check_duplicates( self,label, name ):
      if self.graph.find_one(label ,property_key="name",property_value=name) != None:
         raise ValueError( "Duplicate Node",label,name )


   def site( self,name=None,*args):
       self.check_duplicates( "Site",name)
       site = Node("Site",name=name)
       site_connection = Relationship(self.current_system,"SITE",site)
       self.graph.create(site_connection)
       self.current_site = site


   def end_site( self,  *args ):
       self.current_site = None

   def controller( self,name=None,input_queue=None,rpc_queue=None, *args):
       self.check_duplicates( "Controller",name)
       controller = Node("Controller",name=name,input_queue=input_queue,rpc_queue=rpc_queue)
       self.graph.create(controller)
       controller_connection = Relationship(self.current_site,"Queue",controller)
       self.graph.create(controller_connection)
       self.current_controller = controller
   
   def end_controller( self,*args):
       self.current_controller = None
 
   #schedules are  nodes -- no end function required
   def schedules( self, name=None, steps=None,storeage=None, *args):
       self.check_duplicates( "Schedule",name)
       schedule = Node("Schedule",name=name,steps=steps,storeage=storeage)
       self.graph.create(schedule)
       schedule_connection = Relationship(self.current_controller,"Schedules",schedule_connection)
       self.graph.create(schedule_connections)
     



   #events are  nodes -- no end function required 
   def events( self, name=None,storeage=None, *args ):
       self.check_duplicates( "Events",name)
       events = Node("Events",name=name,storeage=storeage)
       self.graph.create(events)
       event_connection = Relationship(self.current_controller,"Events",events)
       self.graph.create(event_connection)
      
   #alarms are  nodes -- no end function required 
   def alarms( self, name=None,storeage=None, *args ):
       self.check_duplicates( "Alarms",name)
       events = Node("Alarms",name=name,storeage=storeage)
       self.graph.create(events)
       event_connection = Relationship(self.current_controller,"Alarms",events)
       self.graph.create(event_connection)


   def interfaces( self, controller, *args ):
      pass

   def remotes( self, interfaces, *args ):
      pass

   def io( self,remote, *args ):
      pass

class Node_Queries:

   def __init__(self,graph):
       self.graph = graph

   def find_sites( self,*args):
       return_value = []
       for i in self.graph.find("Site"):
          return_value.append(i.properties)
       return return_value

   def find_controllers_at_site(self,site=None):
       site_node = self.graph.find_one("Site","name",site)
       return_value = []
       for i in self.graph.match(site_node,"Controller"):
          return_value.append( i.end_node.properties )
       return return_value

   def find_controller_inqueue( self, input_queue = None):
       return_value = []
       for i in self.graph.find( "Controller","input_queue",input_queue):
         return_value.append(i.properties)
       return return_value


if __name__ == "__main__" :
    nc = Node_Constructor()
    nq = Node_Queries(nc.graph)
    ut = Neo4j_Utilities( nc.graph )
    nc.site(name="LaCima")
    nc.controller(name="IRRIGATION",input_queue="input_1",rpc_queue="rpc_1")
    print nq.find_sites()
    print nq.find_controllers_at_site(site="LaCima")
    print nq.find_controller_inqueue(input_queue="input_1")
    print nq.find_controller_inqueue(input_queue="input_2")  #should result in empty list
 
    
    
