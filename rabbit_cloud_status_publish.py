#!/usr/bin/env python

import json
import time
import redis
import logging


          


if __name__ == "__main__":
  
   import pika
   import time
   import pika
   import json
   import time
   import os

   def callback(ch, method, properties, body):
         print(" [x] %r" % body)


       
   credentials = pika.PlainCredentials( 'LaCimaRemote', 'xS2Wf[+bb.3U>KsM' )
   parameters = pika.ConnectionParameters( 'localhost',
                                           5671,  #ssl port
                                           "LaCima",
                                           credentials,
                                           ssl = True )
 
                                           
   connection = pika.BlockingConnection(parameters)
        
   channel = connection.channel()
   channel.exchange_declare(exchange='status_queue',
                         type='fanout')
   result = channel.queue_declare(exclusive=True)
   queue_name = result.method.queue
   
   channel.queue_bind(exchange="status_queue",
                      queue=queue_name)

   channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)
   print(' [*] Waiting for messages. To exit press CTRL+C')
   channel.start_consuming()



