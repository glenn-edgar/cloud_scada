#
#  The purpose of this file is to load a system configuration
#  in the graphic data base
#

from neo4j_graph.graph_functions import Build_Configuration
from neo4j_graph.farm_template   import Construct_Farm
 
if __name__ == "__main__" :
   bc = Build_Configuration()
   cf = Construct_Farm(bc)
   
   #
   #
   # Construct Systems
   #
   #
   cf.construct_system("LaCima Operations")

   #
   #
   # Construction Sites for LaCima
   #
   #

   cf.construct_site( name="LaCima",wired=True,address="21005 Paseo Montana Murrieta, Ca 92562")

   #
   #  Constructing Controllers
   #
   #
   #
   cf.construct_controller( "PI_1","rpc_queue","alert_status_queue","192.168.1.82","irrigation/1","LaCima")
   cf.add_event_queue()
   cf.add_system_event_queue()

   # need to automatically construct these files in the future
   cf.add_schedule( name='fruit_tree_low_water',number=11)
   cf.add_schedule( name='flowers',number=14)
   cf.add_schedule( name='clean-filter',number=6)
   cf.add_schedule( name='house',number=5)

   cf.add_flow_sensor(name='main_sensor',controller='satellite_1',io=1,conversion_factor = 0.0224145939)
   

   #
   #  Contructing IO Devices
   #  a remote has to be attached to a controller
   #  Multiple controllers can interface to udp server but not to same controller
   #

   cf.add_udp_io_sever(name="main_remote", ip="192.168.1.82",remote_type= "UDP", port=5005 )
   cf.add_rtu_interface(name = "rtu_2",protocol="modify_modbus",baud_rate=38400 )
   cf.add_remote(  name="satellite_1",modbus_address=100,irrigation_station_number=44 )
   cf.add_remote(  name="satellite_2",modbus_address=125 ,irrigation_station_number=22)
   cf.add_remote(  name="satellite_3",modbus_address=170,irrigation_station_number=22) 
   cf.end_rtu_interface()
   cf.end_udp_io_server()
   cf.end_controller()
   cf.end_site()
   cf.end_system()

