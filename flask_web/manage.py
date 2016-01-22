#import load_files
import  redis
from functools import wraps
from werkzeug.contrib import authdigest
import flask

from flask import Flask
from flask import render_template,jsonify,request
from flask import request, session

#from app.flow_rate_functions    import *
#from app.system_state           import *
#from app.statistics_modules     import *
#from app.template_support       import *
import os
import json
import sys
#import io_control.modbus_UDP_device


redis                 = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 )   
vhost   = sys.argv[1]
startup_dict = redis.hgetall(vhost)


class FlaskRealmDigestDB(authdigest.RealmDigestDB):
    def requires_auth(self, f):
        @wraps(f)
        def decorated(*args, **kwargs):
            request = flask.request
            if not self.isAuthenticated(request):
                return self.challenge()

            return f(*args, **kwargs)

        return decorated

app = Flask(__name__)
app.config['SECRET_KEY']      = startup_dict["SECRET_KEY"]
app.config["DEBUG"]           = startup_dict["DEBUG"]
app.template_folder           = None
app.static_folder             = 'static'

authDB = FlaskRealmDigestDB(startup_dict["RealmDigestDB"])
temp =  json.loads(startup_dict["users"])
for i in temp:
    authDB.add_user(i["user"], i["password"] )


  
class FlaskRealmDigestDB(authdigest.RealmDigestDB):
    def requires_auth(self, f):
        @wraps(f)
        def decorated(*args, **kwargs):
            request = flask.request
            if not self.isAuthenticated(request):
                return self.challenge()

            return f(*args, **kwargs)

        return decorated




@app.route('/',methods=["GET"])
@authDB.requires_auth
def home():
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page("/")
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"

@app.route('/index.html',methods=["GET"])
@authDB.requires_auth
def index():
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page("/")
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"

'''
 
######################### Set up Static RoutesFiles ########################################


  
@app.route('/site_map/<int:map_type>',methods=["GET"])
@authDB.requires_auth
def site_map(map_type):  
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page('/site_map/'+str(map_type))
   
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"
 
@app.route('/static/js/<path:filename>')
@authDB.requires_auth
def get_js(filename):
   station_control = get_rabbit_interface( vhost )
   
   return_value = station_control.get_web_page('/static/js/'+filename)
   print "get_js",return_value[0]
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"

@app.route('/static/js_library/<path:filename>')
@authDB.requires_auth
def get_js_library(filename):
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page('/static/js_library/'+filename)
   print "js_library",filename,return_value[0]
   if return_value[0] == True:
      return return_value[1]
   else:
      
      return "No Connections"

@app.route('/static/css/<path:filename>')
@authDB.requires_auth
def get_css(filename):
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page('/static/css/'+filename)
   print "filename",'/static/css/'+filename, return_value[0]
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"


@app.route('/static/images/<path:filename>')
@authDB.requires_auth
def get_images(filename):
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page('/static/images/'+filename)
   
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"


@app.route('/static/dynatree/<path:filename>')
@authDB.requires_auth
def get_dynatree(filename):
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page('/static/dynatree/'+filename)
   
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"


@app.route('/static/themes/<path:filename>')
@authDB.requires_auth
def get_themes(filename):
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page('/static/themes/'+filename)
   print "themes",filename,return_value[0]
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"


@app.route('/static/html/<path:filename>')
@authDB.requires_auth
def get_html(filename):
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page('/static/html/'+filename)
   
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"


@app.route('/static/app_images/<path:filename>')
@authDB.requires_auth
def get_app_images(filename):
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page('/static/app_images/'+filename)
   
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"

'''


@app.route('/<path:path>',methods=["GET"])
@authDB.requires_auth
def remote_files_get(path):
   
   station_control = get_rabbit_interface( vhost )
   return_value = station_control.get_web_page("/"+path)
   
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"

@app.route('/<path:path>',methods=["POST"])
@authDB.requires_auth
def remote_files_post(path):
   param              = request.get_json()
   station_control = get_rabbit_interface( vhost )
   
   return_value = station_control.post_web_page( "/"+path, param )
   if return_value[0] == True:
      return json.dumps( return_value[1] )
   else:
      return "No Connections"
   


def get_rabbit_interface( vhost ):
   username           = startup_dict["rabbit_username"]
   password           = startup_dict["rabbit_password"]
   port               = int(startup_dict["rabbit_port"])
   server             = startup_dict["rabbit_server"]
   queue              = startup_dict["rabbit_queue"]
   time_out           = 10
   rabbit_interface   =  RabbitMq_Client( server, port, username, password, vhost, queue )
   station_control.set_rpc( rabbit_interface, time_out )        
   return station_control

                                
import sys
from rabbitmq_client             import *
from station_control             import *
station_control = Station_Control()
if __name__ == '__main__':
   
   app.run(threaded=True , use_reloader=True, host='0.0.0.0' , port=int(startup_dict["web_port"]), ssl_context=(startup_dict["crt_file"], startup_dict["key_file"] ) )



   
