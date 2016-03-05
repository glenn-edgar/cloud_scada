 
import redis
import json
from slacker import Slacker









# This is a test module no need for intermediate code










if __name__ == "__main__":



   redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   slack_json = redis_startup.hget("MEDIA_DRIVERS","Slack")
   slack_dict   = json.loads( slack_json )


   token           = slack_dict["token"]
   print "token",token
   slack = Slacker(token)
   # Send a message to #general channel
   slack.chat.post_message('#general', 'Hello fellow slackers!', as_user=False)
   try:
       slack.channels.create("#laCima-Issues")
   except:
       print "channel not created"





