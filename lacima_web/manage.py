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
                                

from rabbitmq_client             import *
from station_control             import *


redis_handle                 = redis.StrictRedis( host = "localhost", port=6379, db = 2 )   
rabbit_vhost   = sys.argv[1]
startup_dict = redis_handle.hgetall(rabbit_vhost)
rabbit_username           = startup_dict["rabbit_username"]
rabbit_password           = startup_dict["rabbit_password"]
rabbit_port               = int(startup_dict["rabbit_port"])
rabbit_server             = startup_dict["rabbit_server"]
rabbit_queue              = startup_dict["rabbit_queue"]
time_out           = 10


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

   station_control = get_rabbit_interface()
   return_value = station_control.get_web_page("/index.html")

   if return_value[0] == True:

      return return_value[1]
   else:

      return "No Connections"

@app.route('/index.html',methods=["GET"])
@authDB.requires_auth
def index():
   station_control = get_rabbit_interface(  )
   return_value = station_control.get_web_page("/")
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"

 
######################### Set up Static RoutesFiles ########################################


 ######################### Set up Static RoutesFiles ########################################

   
@app.route('/favicon.ico')
@authDB.requires_auth
def send_favicon():
  return app.send_static_file("favicon.ico")
   

@app.route('/static/js/<path:filename>')
@authDB.requires_auth
def get_js(filename):
  return app.send_static_file(os.path.join('js', filename))

@app.route('/static/js_library/<path:filename>')
@authDB.requires_auth
def get_js_library(filename):
 return app.send_static_file(os.path.join('js_library', filename))

@app.route('/static/css/<path:filename>')
@authDB.requires_auth
def get_css(filename):
 return app.send_static_file(os.path.join('css', filename))

@app.route('/static/images/<path:filename>')
@authDB.requires_auth
def get_images(filename):
 return app.send_static_file(os.path.join('images', filename))

@app.route('/static/dynatree/<path:filename>')
@authDB.requires_auth
def get_dynatree(filename):
 return app.send_static_file(os.path.join('dynatree', filename))

@app.route('/static/themes/<path:filename>')
@authDB.requires_auth
def get_themes(filename):
 return app.send_static_file(os.path.join('themes', filename))

@app.route('/static/html/<path:filename>')
@authDB.requires_auth
def get_html(filename):
 return app.send_static_file(os.path.join('html', filename))

@app.route('/static/app_images/<path:filename>')
@authDB.requires_auth
def get_app_images(filename):
 return app.send_static_file(os.path.join('app_images', filename))



#@app.route('/static/html/<path:filename>')
#@authDB.requires_auth
#def get_html(filename):
# return render_template(filename)

@app.route('/static/data/<path:filename>')
@authDB.requires_auth
def get_data(filename):
  return app.send_static_file(os.path.join('data', filename))





@app.route('/<path:path>',methods=["GET"])
@authDB.requires_auth
def remote_files_get(path):
   
   station_control = get_rabbit_interface( )
   return_value = station_control.get_web_page("/"+path)
   
   if return_value[0] == True:
      return return_value[1]
   else:
      return "No Connections"

@app.route('/<path:path>',methods=["POST"])
@authDB.requires_auth
def remote_files_post(path):
   param              = request.get_json()
   station_control = get_rabbit_interface(  )
   
   return_value = station_control.post_web_page( "/"+path, param )
   if return_value[0] == True:
      return json.dumps( return_value[1] )
   else:
      return "No Connections"
   


def get_rabbit_interface(  ):
   rabbit_interface   =  RabbitMq_Client( rabbit_server, rabbit_port, rabbit_username, rabbit_password, rabbit_vhost, rabbit_queue )
   station_control.set_rpc( rabbit_interface, time_out )        
   return station_control

station_control = Station_Control()
if __name__ == '__main__':
   
   app.run(threaded=True , use_reloader=True, host='0.0.0.0' , port=int(startup_dict["web_port"]), ssl_context=(startup_dict["crt_file"], startup_dict["key_file"] ) )



   
