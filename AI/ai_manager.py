#
# File: ai_manager.py
#
#
#
#
#
import pika
import uuid
import json
import base64
import datetime
import time
import string
import urllib2
import math
import redis
import json
import py_cf
import os

from rabbit.rabbitmq_client import *
from rabbit.client_commands import *




def create_rabbit_mq_connection( vhost="",queue=""):
   redis_startup         = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
   username              = redis_startup.hget("AI_rabbitmq","username" )
   password              = redis_startup.hget("AI_rabbitmq","password" )
   server                = redis_startup.hget("AI_rabbitmq","server"   )
   port                  = int(redis_startup.hget("AI_rabbitmq","port"     ) )
   queue                 = redis_startup.hget("AI_rabbitmq","queue" )
   client                = RabbitMq_Client(server,port,username,password,vhost,queue)
   station_control       = Status_Alert_Cmds( )
   station_control.set_rpc( client , 10 ) 
   return station_control

if __name__ == "__main__":
   redis_startup                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
 


   cf.define_chain("Initialize_System",True)
   cf.insert_link( "link_1","One_Step",[initialize_graph_database] )
   cf.insert_link( "link_2","One_Step",[initialize_rabbitmq_connections ] )
   cf.insert_link( "link_3","Enable_Chain",["Check_Online","Check_Alerts","Log_Events","Check_Irrigation","Check_Resistance","Off_Flow_Check","Check_Alarm_Aggregators"])
   cf.insert_link( "link_3","Reset",[] )


   cf.define_chain("Check_Online",False)
   cf.insert_link( "link_1","WaitTime",[5,0 ])
   cf.insert_link( "link_2","Reset",[] )
     
   cf.define_chain("Check_Alerts",False)
   cf.insert_link( "link_1","WaitTime",[5,0 ])
   cf.insert_link( "link_2","Reset",[] )

   cf.define_chain("Log_Events",False)
   cf.insert_link( "link_1","WaitTime",[5,0 ])
   cf.insert_link( "link_2","Reset",[] )

   cf.define_chain("Check_Irrigation",False)
   cf.insert_link( "link_1","WaitTime",[5,0 ])
   cf.insert_link( "link_2","Reset",[] )

   cf.define_chain("Check_Resistance",False)
   cf.insert_link( "link_1","WaitTime",[5,0 ])
   cf.insert_link( "link_2","Reset",[] )

   cf.define_chain("Off_Flow_Check",False)
   cf.insert_link( "link_1","WaitTime",[5,0 ])
   cf.insert_link( "link_2","Reset",[] )

   cf.define_chain("Check_Alarm_Aggregators",False)
   cf.insert_link( "link_1","WaitTod",["*","*",30,"*" ])
   cf.insert_link( "link_2","Reset",[] )


 

   cf_environ = py_cf.Execute_Cf_Environment( cf )
   cf_environ.execute()



