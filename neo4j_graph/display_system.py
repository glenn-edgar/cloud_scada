#
#  For Diagnostics Purposes to test 
#
#
#
#
#
from query_farm      import Query_Farm
from graph_functions import Query_Configuration


if __name__ == "__main__" :
   qc = Query_Configuration()
   qf = Query_Farm(qc)

   
   systems = qf.find_systems()
   print "systems",systems[0].properties 
  
   sites = qf.find_sites()
   print "sites",sites[0].properties 
  
   controllers =  qf.find_controllers(  )
   print "controllers",controllers[0].properties
   alert_queues =  qf.find_alert_queues()
   print "alert_queues",alert_queues[0].properties
   event_queues =  qf.find_event_queues()
   print "event_queues",event_queues[0].properties
   system_alert_queues =  qf.find_system_alert_queues(  )
   print "system_alert_queues",system_alert_queues[0].properties
   system_event_queues =   qf.find_system_event_queues( )
   print "system_event_queues",system_event_queues[0].properties
   schedules =   qf.find_schedules(  )
   print "schedules",schedules[0].properties
   flow_sensors =   qf.find_flow_sensors(  )
   print "flow_sensors",flow_sensors[0].properties
   remote_servers =   qf.find_remote_servers(  )
   print "remote_servers",remote_servers[0].properties
   modbus =   qf.find_modbus_rtu_serial_interface(  )
   print "modbus",modbus[0].properties
   remotes=   qf.find_rtu_units(  )
   print "remotes",remotes[0].properties  

   controllers = qc.match_label_property( "CONTROLLER", "name","PI_1")
   print controllers[0]
   qc.modify_properties( controllers[0], {"a":"a value","b":"b value"} )

   controllers = qc.match_label_property( "CONTROLLER", "name","PI_1")
   print controllers[0]
   qc.modify_properties( controllers[0], {"a":"a new value","b":"b new value"} )

   controllers = qc.match_label_property( "CONTROLLER", "name","PI_1")
   print controllers[0]


   
