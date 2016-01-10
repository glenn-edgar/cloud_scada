#
#  
#  File: passwords.py --- IMPORTANT only a template file
#
#  fill in correct data
#  run first thing after startup
#  we are assuming that mongodb, redis.io, and neo4j are
#  set up with no athenication and run locally on the target machine
#  if not then addition fields will have to be setup
#  ---- IMPORTANT:  Donot put the actual version of this file in
#                   the configuration, ie, not in new_python directory tree


import redis
import json
redis                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )
redis.hset("AI_rabbitmq","username", "xxxxx"        )
redis.hset("AI_rabbitmq","password", 'xxxx'     )
redis.hset("AI_rabbitmq","port",      5671           ) 
redis.hset("AI_rabbitmq","server",    "xxxxx")
redis.hset("AI_rabbitmq","queue"  ,    'alert_status_queue' )

redis.hset("Master_Web","crt_file", 'xxxxxxx')
redis.hset("Master_Web","key_file",'xxxxxx')
redis.hset("Master_Web","SECRET_KEY",'xxxxxx')
redis.hset("Master_Web","DEBUG",False)
redis.hset("Master_Web","RealmDigestDB",'xxxxxx')
redis.hset("Master_Web","users",json.dumps([{ "user":"xx","password":"xxxx" } ]))
#
#
#  Proxy Web Server Setups
#
#
redis.delete("vhosts")
#LaCima Site
redis.rpush("vhosts","xxxx")
redis.hset("LaCima","crt_file", 'xxxxx')
redis.hset("LaCima","key_file",'xxxxx')
redis.hset("LaCima","SECRET_KEY",'xxxxx')
redis.hset("LaCima","DEBUG",False)
redis.hset("LaCima","RealmDigestDB",'xxxx')
redis.hset("LaCima","users",json.dumps([{ "user":"xxxx","password":"xxx" } ]))
redis.hset("LaCima","web_port",'1025')
redis.hset("LaCima","rabbit_username",'xxxxx')
redis.hset("LaCima","rabbit_password",'xxxxx')
redis.hset("LaCima","rabbit_port",5671 )
redis.hset("LaCima","rabbit_server",'xxxxxx')
redis.hset("LaCima","rabbit_queue",'xxxxx')

