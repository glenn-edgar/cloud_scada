
import datetime
import json
import redis
import tweepy



import tweepy
redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
tweeter_json = redis_startup.hget("MEDIA_DRIVERS","TWITTER")
tweeter_dict = json.loads( tweeter_json)



class Twitter_Driver():

   def __init__( self ):
       
       self.cfg   = tweeter_dict
       self.title = self.cfg["title"]
       self.api   = self.get_api()

   def get_api( self ):
         auth = tweepy.OAuthHandler(  self.cfg['consumer_key'], self.cfg['consumer_secret'])
         auth.set_access_token(self.cfg['access_token'], self.cfg['access_token_secret'])
         return tweepy.API(auth)

   def send( self, msg_type, msg):
       tweet  = self.title + "\n"+msg_type+" : "+str(datetime.datetime.today())+"--->"+msg
       try:
           result = self.api.update_status(status=tweet)
       except:
           print "tweet exception occurred"






if __name__ == "__main__":
  td = Twitter_Driver()
  td.send("Alert","Test Alert Message")
 
  
