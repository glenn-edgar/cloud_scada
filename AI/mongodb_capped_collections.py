import copy
import json
import pymongo
from pymongo import MongoClient


from mongodb.collection_functions import Mongodb_Collection
from mongodb.collection_functions import Mongodb_DataBase


class Capped_Collections:

   def __init__(self, mongodb_db, mongodb_col, db_name,    max_number , size):
       self.db_name     = db_name
       self.size        = size
       self.max_number  = max_number
       mongodb_db.create_database(db_name)  
       self.mongodb_db  = mongodb_db
       self.mongodb_col = mongodb_col

   def create( self, collection_name ):
       if self.mongodb_col.collection_exits( self.db_name, collection_name  ) == False:
           self.mongodb_col.create_collection( self.db_name, collection_name, capped=True, max_number= self.max_number, collection_size = self.size )
       else:
           print collection_name + "  Queue not created"


   def get_tail_documents( self, collection_name,number):
       return self.mongodb_col.collection_tail(  db_name=self.db_name, collection_name=collection_name, query_number=number  )

   def insert( self, collection_name, document ):
       self.mongodb_col.insert_document( self.db_name, collection_name,[document] )
 
   

   def number( self, collection_name ):
       return self.mongodb_col.collection_number( self.db_name, collection_name)




if __name__ == "__main__":
   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
   mongodb_db.remove_all_databases()
   cc              = Capped_Collections( mongodb_db, mongodb_col, "test", 5, 1024 )
   cc.create("test_1")
   cc.create("test_2")
   cc.create("test_1")
   print cc.number("test_1")
   print "tail documents",cc.get_tail_documents("test_1",5)
   cc.insert( "test_1", {"a":1} )
   cc.insert( "test_1", {"a":2} )
   cc.insert( "test_1", {"a":3} )
   cc.insert( "test_1", {"a":4} )
   cc.insert( "test_1", {"a":5} )
   cc.insert( "test_1", {"a":6} )
   cc.insert( "test_1", {"a":7} )
   print cc.number("test_1")
   print "tail documents",cc.get_tail_documents("test_1",5)
   print "tail documents",cc.get_tail_documents("test_1",7)
   print "tail documents",cc.get_tail_documents("test_1",2)
   mongodb_db.remove_all_databases()
