# file: redis_graph_common.py
# the purpose of this file is to implement common graph functions

import redis
import copy

class Redis_Graph_Common:
   #tested
   def __init__( self, redis, separator = ":", relationship_sep="#", label_sep="~", header_end=";" ):
      self.redis     = redis
      self.sep       = separator
      self.rel_sep   = relationship_sep
      self.label_sep = label_sep
      self.header_end  = header_end

   def _make_string_key( self, relationship,label,name):
       return relationship+self.rel_sep+label+self.label_sep+name+self.header_end
 
   def _convert_namespace( self, namespace):
       temp_value = []
       for i in namespace:
          temp_value.append(self._make_string_key( i[0],i[1],i[2] ))
       key_string = self.sep+self.sep.join(temp_value)
       return  key_string
  
   def construct_node( self, namespace, relationship,label,name ): #tested 
       
       new_name_space = copy.copy(namespace)
       new_name_space.append( [ relationship,label,name ] )
       
       redis_string = self._convert_namespace( new_name_space )
       redis.hset(redis_string,"name",name)
       redis.hset(redis_string,"namespace",self._convert_namespace(new_name_space))
       return redis_string, new_name_space

   

   def match( self, relationship, label, name , starting_path=None):
       match_string =self._convert_namespace([[relationship,label,name]])
       if starting_path != None:
          start_string = self._convert_namespace(starting_path) 
       else:
          start_string = ""
       match_key = start_string+"*"+match_string
       return redis.keys( match_key )

   def delete_all(self): #tested
       keys = redis.keys(self.sep+"*")
       for i in keys:
          redis.delete(i)


if __name__ == "__main__":
   # test driver
   redis  = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 11 )   
   common = Redis_Graph_Common( redis)
   redis_key, new_namespace =common.construct_node( [], "","head","head" )
   print redis_key,new_namespace
   print redis.hgetall(redis_key)

   redis_key, new_namespace =common.construct_node( new_namespace,"relation 1","level_one","h1" )
   print redis_key,new_namespace
   print redis.hgetall(redis_key)
   redis_key, new_namespace =common.construct_node(  new_namespace,"relation 2","level_two","h2" )
   print redis_key,new_namespace
   print redis.hgetall(redis_key)

   print "simple match"
   print common.match( "relation 2","level_two","h2")
   print "starting match"
   print common.match( "*","level_two","h2",[["","head","head"]])

   print "all the keys"
   print redis.keys("*")
   print "none of the keys"
   common.delete_all()
   print redis.keys("*")
