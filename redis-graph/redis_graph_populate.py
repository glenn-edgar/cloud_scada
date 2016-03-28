import redis
import redis_graph_common


class Build_Configuration():

   def __init__( self, redis_graph_common ):
      self.common = redis_graph_common
      self.common.delete_all()
      self.namespace     = []
 
   def get_namespace( self,name ):
       return_value = copy.deepcopy(self.namespace) 
       return_value.append(name)
       return return_value


   def pop_namespace( self ):
       del self.namespace[-1]    


   # concept of namespace name is a string which ensures unique name
   # the name is essentially the directory structure of the tree
   def construct_node(self, push_namespace,relationship, label, name, properties ):
 
      
       redis_key, new_name_space = self.common_construct_node( self.get_namespace, relationship,label,name ) 
       for i in properties.keys:
           redis.hset(i,properties[i] )
       
       
       if push_namespace == True:
          self.name_space = new_name_space
       
       


