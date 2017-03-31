#!/usr/bin/env python

import json
import time
import redis
import logging

import influxdb_interface





          


if __name__ == "__main__":
  
   import pika
   import time
   import pika
   import json
   import time
   import os

   influx_client = influxdb_interface.Influx_Interface()

   def callback(ch, method, properties, json_data):
        
         data  = json.loads(json_data)
         
         if data.has_key("routing_key")  == True:
            print "routing_key",data["routing_key"]
            influx_client.process_messages( data["routing_key"], data, json_data)
         else:
             print "no routing key"


       
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



