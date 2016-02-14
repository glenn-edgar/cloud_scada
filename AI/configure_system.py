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
   cf.construct_controller( "PI_1","rpc_queue","alert_status_queue","192.168.1.82","irrigation/1","LaCima",
   {"temperature":"Main Controller Temperature","ping":"Main Controller Connectivity","irrigation_resets":"Main Controller Irrigation Resets","system_resets":"Main Controller System Resets" },
    "CONTROLLER_STATUS")
   cf.add_event_queue()
   cf.add_system_event_queue()
   cf.add_diagnostic_card_header()
   org_name =    "LaCima Ranch"
   list_name =   "PI_1 Irrigation Controller"
   board_name  = "System Operation"
   cf.add_diagnostic_card(org_name,board_name,list_name,"Main Controller Temperature" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Main Controller Connectivity" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Main Controller Irrigation Resets" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Main Controller System Resets" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 1 Connectivity" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 2 Connectivity" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 3 Connectivity" )
   board_name  = "Irrigation Electrical Wiring"
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 1 Open Wire" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 2 Open Wire" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 3 Open Wire" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 1 Shorted Selenoid" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 2 Shorted Selenoid" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 3 Shorted Selenoid" )
   board_name  = "Irrigation Plumbing"
   cf.add_diagnostic_card(org_name,board_name,list_name,"Clean Filter" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Off Flow Rate" )
   board_name = "Irrigation Schedules"
   cf.add_diagnostic_card(org_name,board_name,list_name,"fruit_trees_low_water" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"house" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"flowers" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"clean-filter" )
   cf.end_diagnostic_card_header()
       

   cf.add_flow_sensor_header()
   cf.add_flow_sensor(name='main_sensor',controller='satellite_1',io=1,conversion_factor = 0.0224145939)
   cf.end_flow_sensor_header(  )

   # need to automatically construct these files in the future
   cf.add_schedule_header()
   cf.add_schedule( name='fruit_trees_low_water',number=11,flow_sensor_names = ['main_sensor'])
   cf.add_schedule( name='flowers',number=14,flow_sensor_names = ['main_sensor'])
   cf.add_schedule( name='clean-filter',number=6,flow_sensor_names = ['main_sensor'])
   cf.add_schedule( name='house',number=5,flow_sensor_names = ['main_sensor'])
   cf.end_schedule_header()

   

   #
   #  Contructing IO Devices
   #  a remote has to be attached to a controller
   #  Multiple controllers can interface to udp server but not to same controller
   #

   cf.add_udp_io_sever(name="main_remote", ip = "192.168.1.82", redis_key="MODBUS_STATISTICS:127.0.0.1",remote_type= "UDP", port=5005   )
   cf.add_rtu_interface(name = "rtu_2",protocol="modify_modbus",baud_rate=38400 )
   cf.add_remote(  name="satellite_1",modbus_address=100,irrigation_station_number=44, card_dict={"open":"Remote 1 Open Wire","short":"Remote 1 Shorted Selenoid","connectivity":"Remote 1 Connectivity"})
   cf.add_remote(  name="satellite_2",modbus_address=125 ,irrigation_station_number=22,card_dict={"open":"Remote 2 Open Wire","short":"Remote 2 Shorted Selenoid","connectivity":"Remote 2 Connectivity"})
   cf.add_remote(  name="satellite_3",modbus_address=170,irrigation_station_number=22,card_dict={"open":"Remote 3 Open Wire","short":"Remote 3 Shorted Selenoid","connectivity":"Remote 3 Connectivity"}) 
   cf.end_rtu_interface()
   cf.end_udp_io_server()
   cf.end_controller()
   cf.end_site()
   cf.end_system()

