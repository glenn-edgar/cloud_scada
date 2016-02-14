from media_drivers.trello_management import Trello_Management 
import redis
import json
from neo4j_graph.graph_functions    import Query_Configuration

class Transfer_Data:

   def __init__( self, trello_management, query_configuration ):
       self.tm = trello_management
       self.qc = query_configuration

   def update_cards( self):
       controller_list = self.qc.match_labels("CONTROLLER")
       for i in controller_list:
            self.update_controller_cards( i )


   def update_controller_cards( self, controller ):
       card_list = self.qc.match_relation_property( "CONTROLLER","namespace",controller.properties["namespace"],"DIAGNOSTIC_CARD")
       for i in card_list:
           pr = i.properties
           print pr["org_name"], pr["board_name"],pr["list_name"],pr["name"]

           card = self.tm.find_card(  pr["org_name"], pr["board_name"],pr["list_name"], pr["name"] )
           
           self.tm.add_card_description(card, pr["description"] )
           self.tm.set_card_label( card, pr["label"] )
           
           try:
               new_commit = json.loads(pr["new_commit"])
               print new_commit
               if type(new_commit) is list:
                   for j in new_commit :
                       self.tm.add_card_comment( card, j )
               else:
                  pass
           except:
               pass
           i.properties["new_commit"] = json.dumps([])
           i.push()
      


 
if __name__ == "__main__":

   redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   trello_json = redis_startup.hget("MEDIA_DRIVERS","TRELLO")
   trello_dict   = json.loads( trello_json )
   tm            = Trello_Management( trello_dict )
   qc            = Query_Configuration()
   td            = Transfer_Data( tm, qc )
   td.update_cards()
