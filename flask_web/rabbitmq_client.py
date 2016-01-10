#!/usr/bin/env python
import pika
import uuid
import json
import base64
import os
import time
import redis
import rabbitmq_client





class RabbitMq_Client(object):
    def __init__(self,server,port,username,password,vhost,queue):
         self.queue     = queue


         credentials = pika.PlainCredentials( username, password )
         parameters = pika.ConnectionParameters( server,
                                                 port,  #ssl port
                                                 vhost,
                                                 credentials,
                                                 ssl = True )
         parameters.heartbeat = 20
         self.connection = pika.BlockingConnection(parameters)        
         self.channel = self.connection.channel()
         result = self.channel.queue_declare(exclusive=True)
         self.callback_queue = result.method.queue
         self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            temp = base64.b64decode(body)
            self.response = json.loads(temp)
            
     

    def call(self, data, time_out ):
      try:
        #print "call time out",time_out
        time_out = time_out *10
        input_data = base64.b64encode(json.dumps(data))

        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key= self.queue,
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body= input_data)
        while self.response is None:
              self.connection.process_data_events()
              if self.response is None:
                 time_out = time_out -1
                 if time_out == 0:
                     data = {}
                     data["reply"] = ["False", "TIME_OUT" ]
                     return data
                 else:
                    time.sleep(.1)
        self.channel.close()
        return self.response
      except:
        return  ["False", "TIME_OUT" ]

    def close( self ):
        self.channel.close()


if __name__ == "__main__":

   redis_startup    = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )   
   username              = redis_startup.hget("AI_rabbitmq","username" )
   password              = redis_startup.hget("AI_rabbitmq","password" )
   server                = redis_startup.hget("AI_rabbitmq","server"   )
   port                  = int(redis_startup.hget("AI_rabbitmq","port"     ) )
   queue                 = redis_startup.hget("AI_rabbitmq","queue" )
   remote_interface = RabbitMq_Client(server,port,username,password,vhost,queue)
   print "rabbitmq queue constructed"
   remote_interface.close()
   print "rabbitmq queue closed"
