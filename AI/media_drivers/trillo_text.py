import redis
import json
import datetime

from twilio.rest import TwilioRestClient
redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
trillo_json = redis_startup.hget("MEDIA_DRIVERS","Trillo")


trillo_dict   = json.loads( trillo_json )

class Twilio_Text():
 
   def __init__( self):
       self.accountSID    = trillo_dict["accountSID"]
       self.authToken     = trillo_dict["authToken"]
       self.twilioNumber  = trillo_dict["twilio_phone_number"]
       self.twilioCli     = TwilioRestClient(self.accountSID, self.authToken )

   def send_text_message( self, phone_number, message ):
       message = "  Date: "+str(datetime.datetime.today()) + "  "+message  
       return self.twilioCli.messages.create( body = message, from_=self.twilioNumber, to=phone_number )


if __name__ == "__main__":
   twilio_text = Twilio_Text()
   status = twilio_text.send_text_message( phone_number = trillo_dict["test_number"], message = "this is a test message") 
   print status.status
