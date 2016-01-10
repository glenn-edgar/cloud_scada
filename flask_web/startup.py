import redis
import os
import subprocess
import json
import time

from jinja2 import Environment, FileSystemLoader

import redis
redis_1                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )

length = redis_1.llen("vhosts")

vhosts     = redis_1.lrange("vhosts",0,-1)
vhost_list = vhosts[1:]
vhost_main = vhosts[0]

env=Environment(loader=FileSystemLoader("./"))
env.get_template("start_apps_temp.txt")
template = env.get_template("start_apps_temp.txt")
result = template.render(vhost_list = vhost_list, vhost_main=vhost_main )
print result
text_file = open("execution.bsh","w")
text_file.write(result)
text_file.close()
subprocess.call("chmod 777 ./execution.bsh",shell=True)
subprocess.call("./execution.bsh",shell=True)
print "done"


