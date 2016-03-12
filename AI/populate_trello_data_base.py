from media_drivers.trello_management import Trello_Management 
import redis
import json
from neo4j_graph.graph_functions    import Query_Configuration
from slacker                        import Slacker


class Transfer_Data:

   def __init__( self, trello_management, query_configuration, slack, slack_channel ):
       self.tm            = trello_management
       self.qc            = query_configuration
       self.slack         = slack
       self.slack_channel = slack_channel


 

   def update_cards( self,*args ):
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
            self.update_controller_cards( i )

   def update_board_color( self, pr ):
       test_color = pr["label"]
       ref_color =  self.tm.board_element["prefs"]["background"]
       if ref_color != "red":  
          if test_color == "red": # make board red
             tm.trello_api.update_board_background_color( self.tm.board_element, "red" ) 
             self.tm.find_board( pr["org_name"], pr["board_name"] )
          elif ref_color == "orange": 
               pass    # board is alread orange
          elif test_color == "yellow":  # board is not orange
              tm.trello_api.update_board_background_color( self.tm.board_element,"orange" )
              self.tm.find_board( pr["org_name"], pr["board_name"] )
          elif ref_color != "green":  # board is blue
              tm.trello_api.update_board_background_color( self.tm.board_element,"green" )     
              self.tm.find_board( pr["org_name"], pr["board_name"] )
          else:
              pass # board is already green
       else:
           pass  # board is alread red
        
   def update_card( self, card_node ):
       pr = card_node.properties
       #print pr["org_name"], pr["board_name"],pr["list_name"],pr["name"],pr["label"]
       card = self.tm.find_card(  pr["org_name"], pr["board_name"],pr["list_name"], pr["name"] )    
       
       self.tm.add_card_description(card, pr["description"] )
       self.tm.set_card_label( card,  pr["label"] )
       
     
       try:
           new_commit = json.loads(pr["new_commit"])
           
           if type(new_commit) is list:
               
               if len( new_commit ) > 0:
                  
                  self.update_board_color( pr )
                
                  if pr["label"] != "green":
                       print "made it here"
                       try:
                           self.slack.channels.create(self.slack_channel )
                       except:
                          pass

                       msg = "Trello Org: "+pr["org_name"]+" Board: "+pr["board_name"]+" List: "+pr["list_name"]+"  Card: "+pr["name"] + "  Alert Level: "+pr["label"]
                       self.slack.chat.post_message(self.slack_channel,msg, as_user=False)                

               
               for i in new_commit :
                   
                   self.tm.add_card_comment( card, i )
           else:
               pass
       except:
           print "exception here"
       card_node.properties["new_commit"] = json.dumps([])
       card_node.push()


   def update_controller_cards( self, controller ):
       self.orgs = {}
       card_list = self.qc.match_relation_property( "CONTROLLER","namespace",controller.properties["namespace"],"DIAGNOSTIC_CARD")
       for i in card_list:
           self.update_card(i)      

   def reset_card_colors( self,chainFlowHandle,chainObj,parameters,event ,color = "green" ):
       card_list = parameters[1]
       for card_id in card_list:
           card = self.tm.find_card(  card_id["org_name"], card_id["board_name"], card_id["list_name"], card_id["card_name"] )
           self.tm.set_card_label( card, "green" )

   
if __name__ == "__main__":

   redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   slack_json = redis_startup.hget("MEDIA_DRIVERS","Slack")
   slack_dict   = json.loads( slack_json )
   token           = slack_dict["token"]
   print "token",token
   slack = Slacker(token)

   trello_json = redis_startup.hget("MEDIA_DRIVERS","TRELLO")
   trello_dict   = json.loads( trello_json )
   tm            = Trello_Management( trello_dict )
   qc            = Query_Configuration()
   td            = Transfer_Data( tm, qc ,slack, "#System-Issues")
   td.update_cards()
