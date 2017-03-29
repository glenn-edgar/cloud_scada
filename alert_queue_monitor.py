# File: alert_queue_monitor.py
# Purpose to distribute alert queue monitors from   various remote units

#!/usr/bin/env python
import pika
import json
import base64
import time
import os
import redis
import logging



class  Receive_Alert_Queue():

   def callback(self, ch, method, properties, body):
       decoded_body   = base64.b64decode(body)
       print(" [x] Received %r" % decoded_body )



   def __init__(self,redis_handle,redis_startup):
        self.redis = redis
        self.user_name = redis_startup.hget("post_alert", "user_name" )
        self.password  = redis_startup.hget("post_alert", "password"  )
        self.vhost     = redis_startup.hget("post_alert", "vhost"     )
        self.queue     = redis_startup.hget("post_alert", "queue"     )
        self.port      = int(redis_startup.hget("post_alert", "port"  ))
        self.server    = redis_startup.hget("post_alert", "server"    )
        
        self.credentials = pika.PlainCredentials( self.user_name, self.password )
        
        parameters = pika.ConnectionParameters( self.server,
                                                self.port,  #ssl port
                                                self.vhost,
                                                self.credentials,
                                                ssl = True )
 
                                           
        connection = pika.BlockingConnection(parameters)
        
        channel = connection.channel()
        channel.queue_declare(queue=self.queue)
        channel.basic_consume( self.callback,
                      queue=self.queue,
                      no_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
  
if __name__ == "__main__":
   redis_startup       = redis.StrictRedis( host = "localhost", port=6379, db = 2 )
   
   redis_password_ip = redis_startup.get("PASSWORD_SERVER_IP")
   redis_password_db = redis_startup.get("PASSWORD_SERVER_DB")
   redis_password_port = redis_startup.get("PASSWORD_SERVER_PORT")
   redis_handle        = redis.StrictRedis( redis_password_ip, 6379, redis_password_db )
   recieve_alert = Receive_Alert_Queue( redis_handle,redis_startup )

