from py2neo import Graph, Node,Relationship
import copy

class Build_Configuration():
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


   # concept of workspace name is a string which ensures unique name
   # the name is essentially the directory structure of the tree
   def construct_node(self, push_workspace,relationship, label, name, properties ):
       workspace_name = self.get_workspace_name(name)
       
       self.check_duplicates( label, name=workspace_name)
       
       node = Node(label)
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
       




class Query_Configuration():

   def __init__( self, graph=None):
        if graph == None:
          self.graph = Graph()

      

   def match_labels( self, label):
       results =  self.graph.cypher.execute("MATCH (m:"+label+")  RETURN m")
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value

   def match_label_property( self, label, prop_index, prop_value):
       results = self.graph.cypher.execute("MATCH (n:"+label+") Where (n."+prop_index+"='"+prop_value+"')  RETURN n")
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value
       

   def match_relationship( self, relationship ):
       query_string = "MATCH n-[:"+relationship+"]->m   RETURN m"  
       #print "---------query string ---------------->"+query_string
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value

   
   # not tested yet
   def cypher_query( self, query_string, return_variable ):
       query_string = query_string + "   RETURN "+return_variable  
       #print "---------query string ---------------->"+query_string
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value

   def modify_properties( self, graph_object, new_properties ):
       for i in new_properties.keys():
          graph_object.properties[i] = new_properties[i]
       graph_object.push()

   # concept of workspace name is a string which ensures unique name
   # the name is essentially the directory structure of the tree
   def construct_merge_node(self, push_workspace,relationship, label, name, new_properties ):
       workspace_name = self.get_workspace_name(name)
       node = self.graph.find_one(label ,property_key="name",property_value=name) 
       if self.graph.find_one(label ,property_key="name",property_value=name) != None:
           for i in properties.keys():
               node.properties[i]=properties[i]
               node.push()
           return node
       else:
           node = Node(label)
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
           return node



   def match_relation_property( self, label_name, property_name, property_value, label ):
       query_string = "MATCH (n:"+label_name+'   { '+property_name +':"'+property_value+'"})-[*]->(o:'+label+')   RETURN o'  
       print "query_string",query_string 
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value
       
'''
nicole = graph.merge_one('Person', 'name', 'Nicole')
nicole['hair'] = 'blonde'
Then you need to push those changes to the graph; cast is inappropriate for updating properties on something that is already a py2neo Node object:

nicole.push()
'''
if __name__ == "__main__" :
   pass
