
from functools import wraps
from werkzeug.contrib import authdigest
import flask
import redis
import os
import json

from flask import Flask
from flask import render_template,jsonify,request
from flask import request, session
redis_startup                = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 1 ) 

startup_dict = redis_startup.hgetall("Master_Web")






redis_handle    = redis.StrictRedis(host='localhost', port=6379 )
  
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
app.template_folder           = "templates"
app.static_folder             = 'static'

authDB = FlaskRealmDigestDB(startup_dict["RealmDigestDB"])
temp =  json.loads(startup_dict["users"])
for i in temp:
    authDB.add_user(i["user"], i["password"] )




######################### Set up Static RoutesFiles ########################################

   
  

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





@app.route("/ajax/get_system_file/<path:file_name>")
@authDB.requires_auth
def get_system_file(file_name):
   
   data = sys_files.load_file(file_name)
   
   return json.dumps(data)

@app.route("/ajax/get_app_file/<path:file_name>")
@authDB.requires_auth
def get_app_file(file_name):
   return json.dumps(app_files.load_file(file_name))

@app.route("/ajax/save_app_file/<path:file_name>",methods=["POST"] )
@authDB.requires_auth
def save_app_file(file_name):
    json_object = request.json
    app_files.save_file(file_name, json_object );
    return json.dumps('SUCCESS')



@app.route('/')
@authDB.requires_auth
def home():
   vhosts_list = redis_startup.lrange("vhosts",0,-1)
   port_list = []
   for i in vhosts_list:
       port = redis_startup.hget(i,"web_port" )
       port_list.append(port)
   print vhosts_list
   print port_list

   return render_template("index",vhosts_list = vhosts_list ,port_list = port_list )
    
@app.route('/index.html')
@authDB.requires_auth
def index():
   vhosts_list = redis_startup.lrange("vhosts",0,-1)
   port_list = []
   for i in vhosts_list:
       port = redis_redis_startup.hget(i,"web_port" )
       port_list.append(port)
   print vhosts_list
   print port_list
   return render_template("index",vhosts_list = vhosts_list ,port_list = port_list )




@app.route('/manage_all_groves')
@authDB.requires_auth
def manage_all_groves():
   return app.send_static_file(os.path.join('html', "index.html"))


                                

if __name__ == '__main__':
  app.run(threaded=True , use_reloader=True, host='0.0.0.0',port=443,
        ssl_context=(startup_dict["crt_file"], startup_dict["key_file"] ))
