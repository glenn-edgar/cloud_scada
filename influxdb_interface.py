#le influxdb interface to Lacima Cloud Data
from  influxdb import InfluxDBClient 
import json
import time

class Influx_Interface(object):

   def __init__( self ):
       self.host      = '127.0.0.1'
       self.port      = 8086
       self.user_name = 'lacimaRanch'
       self.password  = '@lacimaRanch#'
       self.dispatch_dictionary = {}
       temp_dict = {}
       temp_dict["routing_key"] = "moisture_measurement"
       temp_dict["parser"] = self.process_moisture_data
       temp_dict["data_base"] = "moisture_data"
       temp_dict["retention"] = "104w"
       self.dispatch_dictionary["moisture_measurement"] = temp_dict

       temp_dict = {}
       temp_dict["routing_key"] = "eto_measurement"
       temp_dict["parser"] = self.process_eto_data
       temp_dict["data_base"] = "moisture_data"
       temp_dict["retention"] = "104w"
       self.dispatch_dictionary["eto_measurement"] = temp_dict
 
       temp_dict = {}
       temp_dict["routing_key"] = "linux_hour_measurement"
       temp_dict["parser"] = self.process_linux_hourly_data
       temp_dict["data_base"] = "linux_data"
       temp_dict["retention"] = "104w"
       self.dispatch_dictionary["linux_hour_measurement"] = temp_dict

       temp_dict = {}
       temp_dict["routing_key"] = "linux_daily_measurement"
       temp_dict["parser"] = self.process_linux_daily_data
       temp_dict["data_base"] = "linux_data"
       temp_dict["retention"] = "104w"
       self.dispatch_dictionary["linux_daily_measurement"] = temp_dict
     
       for key, dict in self.dispatch_dictionary.items():
          self.form_connection( key, dict )
       #print self.dispatch_dictionary

   


   def process_messages( self, routing_key,data,  json_data ):
       
       #print "routing_key #####################",routing_key
       if self.dispatch_dictionary.has_key(routing_key ):
         temp_dict = self.dispatch_dictionary[routing_key]
         #print "temp_dict",temp_dict
         temp_dict["parser"]( temp_dict["client"], data,json_data )
       else:
         print "event not recognized"


   def form_connection( self, routing_key, dict  ):

       dbname = dict["data_base"]
       retention = dict["retention"]
       
       client = InfluxDBClient( self.host, self.port, self.user_name, self.password, dbname , ssl= True)
      
       dict["client"] = client
       client.create_retention_policy("retention_policy", "104w", "1",dbname, default = True)
       

   def process_linux_hourly_data( self , client, data, json_data ):
     
 
       #print "hourly_data",data.keys()
       #print "linux_memory",data["linux_memory_load"]
       #print "linux_temperature",data["pi_temperature_hourly"]


       print "namespace",data["namespace"]    


       tags                       = {}
       tags["namespace"]          = data["namespace"]
       print data["namespace"]    
       fields                     = {}
   
       fields["linux_memory_load"]             =  json.dumps(data["linux_memory_load"])
       fields["pi_temperature_hourly"]         =  float(data["pi_temperature_hourly"])
       #print "tags", tags
       #print "fields",fields
       influx_body =       [{
                                 "measurement": "linux_hourly",
                                 "time": data["time_stamp"],  # make it a day earlier                                 
                                 "tags": tags,
                                 "fields":     fields
                                 

                              }]
           
        
             
       client.write_points(influx_body)

   def process_linux_daily_data( self , client, data, json_data ):
     
 
       #print "daily_data",data.keys()
       #print "linux_daily_memory",data["linux_daily_memory"]
       #print "linux_daily_disk",data["linux_daily_memory"]
       #print "linux_daily_redis",data["linux_daily_redis"]
      
 
       
       tags                       = {}
       tags["namespace"]          = data["namespace"]
       #print data["namespace"]    
       fields                     = {}
   
       fields['linux_daily_disk']     =  json.dumps( data["linux_daily_disk"] )
       fields["linux_daily_memory"]   =  json.dumps( data["linux_daily_memory"] )
       fields["linux_daily_redis"]    =  json.dumps( data["linux_daily_redis"] )

       #print "tags", tags
       #print "fields",fields
       influx_body =       [{
                                 "measurement": "linux_daily",
                                 "time": data["time_stamp"],  # make it a day earlier                                 
                                 "tags": tags,
                                 "fields":     fields
                                 

                              }]
           
        
             
       client.write_points(influx_body)


   def process_eto_data( self , client, data, json_data ):
     
 
       
       tags                       = {}
       tags["namespace"]          = data["namespace"]
       print data["namespace"]    
       fields                     = {}
   
       fields["eto"]             =  data["eto"]
       fields["rain"]            =  data["rain"]
       print "tags", tags
       print "fields",fields
       influx_body =       [{
                                 "measurement": "eto",
                                 "time": data["time_stamp"],  # make it a day earlier                                 
                                 "tags": tags,
                                 "fields":     fields
                                 

                              }]
           
        
             
       client.write_points(influx_body)
            


   def process_moisture_data( self , client, data, json_data ):
     
       meas = data["measurements"]
       #print "data",data
       #print "data_keys",data.keys()
       #print "meas_keys",meas.keys()
       print "depth_map",data["depth_map"],type(str(data["depth_map"])),type(json.loads(str(data["depth_map"])))

       print "description",data["description_map"],type(str(data["description_map"]))
       print type(data["description_map"])
       
       data["depth_map"] = json.loads(str(data["depth_map"]))
       
       
       tags                       = {}
       tags["namespace"]          = data["namespace"]
       print data["namespace"]    
       fields                     = {}
   
       fields["soil_temperature"] = meas["soil_temperature"]
       fields["air_temperature"] = meas["air_temperature"]
       fields["air_humidity"]    = meas["air_humidity"]
       print meas["time_stamp"]
      
   
       for i in range( len(meas["sensor_data"] )):
           tags["sensor_number"]   = i
           tags["description"]    = data["description_map"][i]  
           tags["configuration"]  = int(meas["sensor_configuration"][i])       
           fields["sensor_number"] = i
           fields["depth"]          = float(data["depth_map"][i])
 
           
           fields["value"]          = float(meas["sensor_data"][i]) 
           fields["resistive"]      = float(meas["resistive_data"][i])

     
           influx_body =       [{
                                 "measurement": "moisture_d",
                                 "time": meas["time_stamp"],                                 
                                 "tags": tags,
                                 "fields":     fields
                                 

                              }]
           
           if float( meas["sensor_configuration"][i]) > 0 :
             
             client.write_points(influx_body)
             print "write",i

if __name__ == "__main__":
  influx_data = Influx_Interface()
  print "done"
