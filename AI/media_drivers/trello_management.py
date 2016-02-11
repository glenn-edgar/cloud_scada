
import redis
import json
from trello_driver import Trello_Api



class Trello_Management:

   def __init__( self, trello_dict ):
       key                       = trello_dict["key"]
       token                     = trello_dict["token"]
       user_name                 = trello_dict["user_name"]
       self.trello_api           = Trello_Api( user_name, key, token )
       self.org_element          = None
       self.board_element        = None
       self.list_element         = None
       self.card_element         = None
       
 


   def format_card( self, *args):  #will have specific parameters
       pass


   def check_element( self, element, field, value ):
       try:
           if element[field] == value:
               return True
       except:
           pass
       return False
   
   def find_organization( self, org_name ):
       if self.check_element( self.org_element,"displayName", org_name ) == False:
          org_element = self.trello_api.create_organization( org_name, "" )
          self.org_element = org_element
       return self.org_element

   def format_org( self,org ):
       description = "This organization hosts status data for system **"+org["displayName"]+"**" 
       self.trello_api.update_org_description(org, description)



   def find_board( self, org_name, board_name ):
       org_element = self.find_organization( org_name )
       if self.check_element( self.board_element,"name", board_name ) == False:
          board_element = self.trello_api.create_board( org_element, board_name,"")
          self.board_element = board_element
          self.list_element = None
          self.card_element = None
          #print "new board fetched"
       else:
          pass
          #print "old board fetched"
       return self.board_element
       
   def format_board( self, board_element, functions ):
       description = "Board **"+board_element["name"]+"**"+"\n"
       description = description +"This board hosts status data for functions"+ "\n"
       for i in range(0, len(functions) ):
         description = description +str(i)+". "+functions[i] +"\n"
       
       self.trello_api.update_board_description( self.board_element, description )



   def find_list( self, organization_name,board_name, list_name ):
       board_element = self.find_board( organization_name, board_name )

       if self.check_element( self.list_element,"name",list_name ) == False:
          list_element = self.trello_api.create_list( board_element , list_name, "" )
          self.list_element = list_element
          self.card_element = None
          #print "new list fetched"
       else:
          pass
          #print "old list fetched"
       return self.list_element

  

   def find_card( self, organization_name, board_name,list_name, card_name ):
       list_element  = self.find_list( organization_name, board_name, list_name)
       if self.check_element( self.card_element, "name",card_name ) == False:
           card_element  = self.trello_api.create_card( list_element, card_name,"" )
           self.card_element = card_element
           #print "new card fetched"
       else:
           pass
           #print "old card fetched"
       
       return self.card_element

   
      
   def add_card_description( self, card_element, text ):
       self.trello_api.update_card_description( card_element,text)

   def add_card_comment(self, card_element, text ):
       self.trello_api.add_card_comment( card_element, text)
   

   def set_card_label( self, card, color ):  # red, yellow, green
       if color in ["red","yellow","green"]:
           self.trello_api.delete_card_label( card, "red")
           self.trello_api.delete_card_label( card, "yellow")
           self.trello_api.delete_card_label( card, "green")
           self.trello_api.add_card_label( card, color )

if __name__ == "__main__":


   redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   trello_json = redis_startup.hget("MEDIA_DRIVERS","TRELLO")
   trello_dict   = json.loads( trello_json )
   trello_mgt    = Trello_Management( trello_dict )
   org_element   = trello_mgt.find_organization( "LaCima" )
   trello_mgt.format_org( org_element)
   board_element = trello_mgt.find_board("LaCima","Electrical Functions" )
   trello_mgt.format_board( board_element,["Check for Sprinkler Wire Breakage","Check for Selenoid Shorts" ]  )
   list_element = trello_mgt.find_list("LaCima","Electrical Functions","LaCima Irrigation Controller" )
   card_element = trello_mgt.find_card("LaCima","Electrical Functions","LaCima Irrigation Controller","Schedule: fruit trees low water")
   
   trello_mgt.trello_api.update_card_description( card_element,"Irrigation Resistance Check \nSchedule: **fruit trees low water**\n-----------------")
   trello_mgt.add_card_comment( card_element,"Step 1:  xxx Pass\n**Step 2:  xxxx Fail** \n-----------------\nStep 3")
   trello_mgt.set_card_label( card_element, "red")

   board_element = trello_mgt.find_board("LaCima","Irrigation Functions" )
   trello_mgt.format_board( board_element,["Check for Sprinkler Wire Breakage","Check for Selenoid Shorts" ]  )
   list_element = trello_mgt.find_list("LaCima","Irrigation Functions","LaCima Irrigation Controller" )
   card_element = trello_mgt.find_card("LaCima","Irrigation Functions","LaCima Irrigation Controller","Schedule: fruit trees low water")
   
   trello_mgt.trello_api.update_card_description( card_element,"Irrigation Resistance Check \nSchedule: **fruit trees low water**\n-----------------")
   trello_mgt.add_card_comment( card_element,"Step 1:  xxx Pass\n**Step 2:  xxxx Fail** \n-----------------\nStep 3")
   trello_mgt.set_card_label( card_element, "red")
 
   trello_mgt.trello_api.delete_board( org_element,"Electrical Functions")
   #trello_mgt.trello_api.delete_organization("LaCima")
 
   trello_mgt.trello_api.delete_board( org_element,"Irrigation Functions" )
   trello_mgt.trello_api.delete_organization("LaCima")

