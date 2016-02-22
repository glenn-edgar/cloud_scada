import json
import pymongo
from pymongo import MongoClient


class Mongodb_DataBase():

   def __init__( self, client ):
      self.client = client
      
   def list_databases(self):
      return self.client.database_names()
   

   def create_database( self,database_name):
      return self.client[database_name]

   def remove_all_databases( self ):
       databases = self.list_databases()
       for i in databases:
          self.remove_database(i)
    

   def remove_database( self,database_name ):
       self.client.drop_database( database_name)


class Mongodb_Collection():

   def __init__(self,client ):
     self.client          = client
     

   def collection_names( self,db_name ):
       db = self.client[ db_name ]
       return db.collection_names()


   def collection_exits( self,db_name, collection_name  ):
     db = self.client[db_name]
     if collection_name in list(db.collection_names()):
          return True
     else:
          return False
       

   def create_collection( self,db_name, collection_name, capped=False, max_number=0,collection_size=0 ):
     db = self.client[db_name]
     if collection_name in list(db.collection_names()):
         pass # print "duplicate collection db "+db_name + " collection name "+ collection_name
     else:
         db.create_collection( collection_name, capped=capped, max=max_number,size=collection_size)
    


   def collection_number( self, db_name, collection_name):
       db = self.client[ db_name ]
       if collection_name in list(db.collection_names()):
          collection = db[ collection_name ]
          return collection.count()
       else:
          raise

    

   
   def delete_collection( self, db_name, collection_name):
       db = self.client[ db_name ]
       if collection_name in list(db.collection_names()):
          collection = db[ collection_name ]
          collection.drop()
       else:
          raise

   def insert_document( self, db_name, collection_name, document_list):
      db = self.client[ db_name ]
     
      if collection_name in list(db.collection_names()):
          collection = db[ collection_name ]
          for i in document_list:
             collection.insert(i)
      else:
          raise

   def find_document( self, db_name="", collection_name="", query="", query_number=0, index=0, sort_field = "", ascending_sort = True  ):
      db = self.client[ db_name ]
      return_value = None
      if collection_name in list(db.collection_names()):
          collection = db[ collection_name ]
          if query_number == 0:
              query_number = collection.count()

          if ascending_sort == True:
              return_value = list(collection.find(query, skip=index , limit=query_number, sort = [[ sort_field, pymongo.ASCENDING ]] )) 
          else:
              return_value = list(collection.find(query, skip=index, limit=query_number, sort=[[ sort_field, pymongo.DESCENDING ]] ))  
      else:
          raise
      return return_value

   def find_head_document( self, db_name="", collection_name="", query_number=1 ):
      db = self.client[ db_name ]
      return_value = None
      if collection_name in list(db.collection_names()):
          collection = db[ collection_name ]
          count = collection.count()
          if count < query_number:
              query_number = count
              index = 0
          else:
              index = count - query_number 
          return_value = list(collection.find({}, skip=index , limit=count )) 
     
      else:
          raise
      return return_value

   def remove_document( self, db_name, collection_name, query  ):
      db = self.client[ db_name ]
      
      if collection_name in list(db.collection_names()):
          collection = db[ collection_name ]
          collection.remove( query )
      else:
          raise
      
 
   def remove_all_documents( self,db_name,collection_name ):
      db = self.client[ db_name ]
      return_value = 0
      if collection_name in list(db.collection_names()):
          collection = db[ collection_name ]
          collection.remove( {} )
      else:
          raise
      
   def modify_documents( self, db_name, collection_name, query, set_query, multi_flag = False):
      db = self.client[ db_name ]
      return_value = 0
      if collection_name in list(db.collection_names()):
          collection = db[ collection_name ]
          collection.update( query, { "$set":set_query } , multi = multi_flag )
      else:
          raise


   def collection_tail( self, db_name, collection_name,  query_number=1 ):
       db = self.client[ db_name ]
       return_value = None
       if collection_name in list(db.collection_names()):
           collection = db[ collection_name ]
           if query_number == 0:
               query_number = collection.count()
           new_index = collection.count() - query_number 
           if new_index < 0 :
               new_index = 0
           
           return_value = list(collection.find({}, skip=0, limit=query_number ))
           #return_value.reverse()
       else:
           raise
       return return_value




if __name__ == "__main__":
   client          = MongoClient()
   mongodb_db      = Mongodb_DataBase( client )
   mongodb_col     = Mongodb_Collection( client)
#   This section tests list and remove data bases
#   print mongodb.list_databases()   
#   mongodb.remove_all_databases()
#   print mongodb.list_databases() 

# create a test data base
   mongodb_db.remove_all_databases()
   print mongodb_db.list_databases()
   mongodb_db.create_database( "test")
 
   mongodb_col.create_collection("test","collection",capped = True, max_number= 255, collection_size = 100000)
   print mongodb_col.collection_number(  "test", "collection" )
   print mongodb_db.list_databases()
   mongodb_col.delete_collection( "test", "collection")
   mongodb_col.create_collection("test","collection",capped= False,max_number= 255, collection_size =100000)
   mongodb_col.insert_document( "test", "collection", [{"name":34,"date":11 }])
   print mongodb_col.collection_number(  "test", "collection" )
   mongodb_col.insert_document( "test", "collection", [{"name":34,"date":22 }])
   mongodb_col.insert_document( "test", "collection", [{"name":34,"date":33 }])
   print "collecton_number",mongodb_col.collection_number(  "test", "collection" )
   print "1",mongodb_col.find_document( db_name="test", collection_name="collection",query={"name":34}, index=1, query_number=1, sort_field = "date", ascending_sort = True  )
   print "2",mongodb_col.find_document( db_name="test", collection_name="collection",query={"name":34}, index=0, query_number=2, sort_field = "date", ascending_sort = True  )
   print "0A",mongodb_col.find_document( db_name="test", collection_name="collection",query={"name":34}, index=0, query_number=0, sort_field = "date", ascending_sort = True  )
   print "0b",mongodb_col.find_document( db_name="test", collection_name="collection",query={"name":34}, index=0, query_number=0, sort_field = "date", ascending_sort = False  )
   print "0c",mongodb_col.find_document( db_name="test", collection_name="collection",query={}, index=0, query_number=0, sort_field = "date", ascending_sort = False  )
   print "--",mongodb_col.find_document( db_name="test", collection_name="collection",query={"date":33}, index=0, query_number=0, sort_field = "date", ascending_sort = False  )
   print "tail",mongodb_col.collection_tail( db_name="test", collection_name = "collection",  query_number=3 )
   print "head",mongodb_col.find_head_document( db_name="test", collection_name = "collection", query_number = 3)
   #print mongodb_col.find_document( "test", "collection",{"name":34}, 0, 0, ascending_sort = False  )
   print mongodb_db.list_databases()

