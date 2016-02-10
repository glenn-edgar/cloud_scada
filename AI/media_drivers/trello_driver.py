


import redis
import json

from trello import TrelloApi
from trello import Members
from trello import Boards
from trello import Cards
from trello import Checklists
from trello import Lists
from trello import Notifications
from trello import Organizations


class Trello_Api:

   def __init__( self, user_name, key, token ):
       self.key                      = key
       self.token                    = token
       self.user_name                = user_name
       self.org_api                  = Organizations(key, token )
       self.board_api                = Boards( key, token )
       self.list_api                 = Lists( key, token )
       self.card_api                 = Cards( key, token )
       self.member_api               = Members( key, token )

   def organization_list_to_dictionary( self,key, convert_list, delete_function ):
       return_value = {}
       for i in convert_list:
        
           if return_value.has_key(i[key]):
               if delete_function != None:
                   delete_function( i["id"] )
                  
           else:
               return_value[i[key]] = i
       return return_value


   def organization_creation_helper_organization( self, name, description, existing_list, create_function ):
   
       if existing_list.has_key(name):
          
          return existing_list[name]
       else:

          return create_function( name,name, description )

   def organization_delete_helper( self, key, enity_dict, delete_function ):
       if enity_dict.has_key(key) :
          delete_function( enity_dict[key]["id"] )



   def list_to_dictionary( self, key, convert_list, delete_function,delete_flag = True ):
       return_value = {}

       for i in convert_list:
           if i["closed"] == False:
               if return_value.has_key(i[key]):
                   if delete_flag == True:
                       delete_function(  i["id"], "true" )
                   else:
                       delete_function( i["id"] )
               else:
                   return_value[i[key]] = i
       return return_value

   


   def creation_helper_parameter( self, name, description, parameter, existing_list, create_function  ):
       
       if existing_list.has_key(name):
          return existing_list[name]
       else:
          #print "creating ---------------------->",name
          if description != None:
             return create_function( name, description, parameter["id"] )
          else:
             return create_function( name, parameter["id"] )

   
   def delete_helper( self, name, enity_dict, delete_function , delete_flag = True ):
       #print enity_dict.keys()
       if enity_dict.has_key(name) :
           element_id = enity_dict[name]["id"]
           if delete_flag == True:
               delete_function( element_id,"true")
           else:
               delete_function( element_id )
   #
   # Organization
   #
   #

   def find_organizations( self ):
       return self.organization_list_to_dictionary( "displayName", self.member_api.get_organization( self.user_name ), self.org_api.delete )
       


   def create_organization( self, name, description ):
       return self.organization_creation_helper_organization( name, description, self.find_organizations(), self.org_api.new )

   def delete_organization( self, name ):
       self.organization_delete_helper( name, self.find_organizations(), self.org_api.delete )

   
   #
   #
   # Boards
   #
   #

   def find_boards( self, organization ):
      return self.list_to_dictionary( "name", self.org_api.get_board( organization["id"] ) , self.board_api.update_closed )

   def create_board( self, organization, name, description ):
       board = self.creation_helper_parameter( name, description, organization, self.find_boards( organization ), self.board_api.new )    
       lists = self.board_api.get_list(board["id"])
       if self.find_boards(organization).has_key( name) == False:
           for i in lists:
               self.list_api.update_closed(i["id"],"true")
       return board

   def delete_board( self, organization, name ):
       self.delete_helper( name, self.find_boards( organization ), self.board_api.update_closed )


   def find_lists( self, board ):
       return self.list_to_dictionary("name", self.board_api.get_list(board["id"]), self.list_api.update_closed )

   def create_list( self, board, name, description ): 
       list_element = self.creation_helper_parameter( name, None, board, self.find_lists( board), self.list_api.new )
       return list_element

   def delete_list( self, board, name ):
       self.delete_helper( name, self.find_lists( board ), self.list_api.update_closed )


   #
   #
   # Cards
   #
   #

   def find_cards( self, list_element ):
       return self.list_to_dictionary("name", self.list_api.get_card(list_element["id"]), self.card_api.delete , False)

   def create_card( self, list_element, name, description ): 
       card_element = self.creation_helper_parameter( name, None , list_element, self.find_cards(list_element), self.card_api.new )
       return card_element



   def delete_card( self, list_element, name ):
       self.delete_helper( name, self.find_cards( list_element ), self.card_api.delete,False )

   def update_description( self, card_element,text):
       self.card_api.update_desc( card_element["id"], text )


   def remove_label( card_element,value ):
       try:
          self.card_api.delete_label_color( value,card_element["id"] )
       except:
          pass

   def add_label( self, card_element, color ):
      try:
          self.card_api.new_label( card_element["id"] , color )
      except:
          pass 

   def delete_label( self, card_element, color):
      try:
          self.card_api.delete_label_color( color, card_element["id"]  )
      except:
          pass

   def add_comment(self, card_element, text ):
       self.card_api.new_action_comment( card_element["id"], text)
      
 


 
if __name__ == "__main__":


   redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   trello_json = redis_startup.hget("MEDIA_DRIVERS","TRELLO")
   trello_dict   = json.loads( trello_json )

   key             = trello_dict["key"]
   token           = trello_dict["token"]
   user_name       = trello_dict["user_name"]
   trello_api      = Trello_Api( user_name, key, token )
   organization = trello_api.create_organization( "test", "test description" )
  
   print trello_api.find_boards( organization ).keys()
   board =  trello_api.create_board( organization, "test board", "test board descriptions" )
   list_element  =  trello_api.create_list( board, "test list", "test list descriptions" )
   print "list_element",list_element
   
   card_element = trello_api.create_card( list_element,"test card","test card descriptions ")
   print "card_element",card_element
   trello_api.update_description( card_element,"new description ")
   trello_api.add_comment( card_element, "This is a new comment")
   trello_api.add_label( card_element,"blue")
   trello_api.add_label( card_element,"black")
   trello_api.delete_label( card_element,"red")
   
   trello_api.delete_card(list_element,"test card" )  
   trello_api.delete_board( organization, "test board")
   trello_api.delete_organization("test")
   

