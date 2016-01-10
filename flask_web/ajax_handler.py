# This is the Ajax handle portion of the cherry library
#
#
#
#

from system_state_module import *
from system_admin_module import *
from limits_module import *
from statistics_modules import *
from rabbitmq_connection_control import *



class Ajax_handler:

  def __init__( self ):
    self.module_dictionary     = {}
    self.connection_control    = Connection_Control()
    self.flow_modules          = System_state_modules( self.module_dictionary , self.connection_control )
    self.sys_admin             = System_admin_modules( self.module_dictionary, self.flow_modules, self.connection_control  )
    self.limit_module          = Limit_Module( self.module_dictionary , self.flow_modules , self.connection_control )
    self.statistics_module     = Statistics_Module( self.module_dictionary , self.flow_modules, self.connection_control )
    
    
  def handle_request(self, url_list, redis_handle, cherrypy ):
     if self.module_dictionary.has_key(url_list) == True :
        return self.module_dictionary[ url_list ]( url_list, cherrypy, redis_handle )
     else:
        raise cherrypy.NotFound
