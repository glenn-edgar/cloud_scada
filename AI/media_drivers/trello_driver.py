


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
redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
trello_json = redis_startup.hget("MEDIA_DRIVERS","TRELLO")
print trello_json
trello_dict   = json.loads( trello_json )



if __name__ == "__main__":
   api_key   = trello_dict["key"]
   token     = trello_dict["token"]
   user_name = trello_dict["user_name"]
   trello_members = Members(api_key, token)
   trello_boards  = Boards( api_key,token)
   boards =  trello_members.get_board(user_name,filter="name")
   board_dict = {}
   for i in boards:
       if i["closed"] == False:
           board_dict[i["name"]] = i
   board_id = board_dict["PSOC 5lp"]["id"]
   print board_id
   cards = trello_boards.get_card(board_id)
   print cards[0].keys()



'''
 'https://trello.com/1/authorize?key=TRELLO_APP_KEY&name=My+App&expiration=30days&response_type=token&scope=read,write'
>>> trello.boards.get('4d5ea62fd76aa1136000000c')
{
    "closed": false,
    "desc": "Trello board used by the Trello team to track work on Trello.  How meta!\n\nThe development of the Trello API is being tracked at https://trello.com/api\n\nThe development of Trello Mobile applications is being tracked at https://trello.com/mobile",
    "id": "4d5ea62fd76aa1136000000c",
    "idOrganization": "4e1452614e4b8698470000e0",
    "name": "Trello Development",
    "pinned": true,
    "prefs": {
        "comments": "public",
        "invitations": "members",
        "permissionLevel": "public",
        "voting": "public"
    },
    "url": "https://trello.com/board/trello-development/4d5ea62fd76aa1136000000c"
}
'''
