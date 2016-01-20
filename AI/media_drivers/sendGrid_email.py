import datetime
import json
import redis
import sendgrid

redis_startup = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
sendGrid_json = redis_startup.hget("MEDIA_DRIVERS","SendGrid")
print sendGrid_json
sendGrid_dict = json.loads( sendGrid_json)


class Send_Email(  ):

     def __init__(self  ):
         self.user = sendGrid_dict["user_name"]
         self.pwd  = sendGrid_dict["password"]
         self.sg   = sendgrid.SendGridClient(self.user,self.pwd )

     def send_message( self, sender,  destination, subject, message ):
       try:
           msg = sendgrid.Mail()
           msg.add_to(destination)
           msg.set_from(sender)
           subject = subject +"  "+ "  Date: "+str(datetime.datetime.today())      
           msg.set_subject(subject)       
           message = "  Date: "+str(datetime.datetime.today()) + "\r\n"+message         
           msg.set_text(message)
           status, error_code = self.sg.send(msg)
           print "email status",status,error_code        
       except:
           print "exception raised email",status,error_code
 
if __name__ == "__main__":
   print datetime.datetime.today()
   sendEmail = Send_Email()
   sendEmail.send_message(destination = sendGrid_dict["test_user"],sender= sendGrid_dict["test_user"],subject = "irrigation_monitoring_test",message = "this is a test message" )

