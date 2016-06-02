import time
import string
import urllib2
import math
import redis
import json
import datetime

class ETO():
   def __init__(self,sites,alt):
       self.sites = sites
       self.alt = alt*0.3048
       self.pressure = 101.3*(((293-.0065*alt)/293)**5.26)
       self.gamma = .000665*self.pressure
       self.reflect = .23


   def convert_do_dictionary( self,result ):
     returnValue = []

     for i in result:
       temp = string.split( i,"<br>")
       working_result = []
       for j in temp:
          temp_a = string.strip(j)
          if temp_a != "":
             temp_b = string.split(temp_a,",")
             working_result.append( temp_b )
 
       fields = working_result.pop(0)

       working_dict_list =  []
       for j in working_result:
         working_entry = {}
         index = 0
         for k in fields: 
             working_entry[ k ] = j[index]
             index = index +1
         working_dict_list.append( working_entry )
       returnValue.append(working_dict_list)
     return returnValue


   def load_data( self, url):
  
      response = urllib2.urlopen(url)
      temp = response.read()
      #print("url data",temp)
      response.close()
      
      return temp

   def get_raw_data_1( self, site, current_time ):
      temp = [ "http://www.wunderground.com/weatherstation/WXDailyHistory.asp?ID=",
               "&day=",
               "&year=",
               "&month=",
               "&format=1" ]
      #print "made it hter"
    
      current_time =  datetime.datetime.fromtimestamp(current_time)
      #print "current_time",current_time.year
      
      assembled_string = temp[0]+site+temp[1]+str(current_time.day)+temp[2]+str(current_time.year)+temp[3]+str(current_time.month)+temp[4]
      return self.load_data( assembled_string )



   def get_raw_data( self, current_time ):
     
     for i in self.sites:
       print "site",i
       try:
         result = []
         temp = self.get_raw_data_1( i, current_time)
         #print "temp",temp 
         result.append(temp)
         result = self.convert_do_dictionary( result )
         if len( result[0] ) > 0 :
            return result
       except:
          pass
     print "sites not found"
     return None


   def parse_time_field( self, input_data ):
     for i in range( 0, len(input_data) ):
        temp = input_data[i]["Time"]
        #2013-11-24 23:42:00
        temp = string.split(temp," ")
        temp_1_list = string.split(temp[0],"-")
        temp_2_list = string.split(temp[1],":")
        year = int(temp_1_list[0])
        month = int( temp_1_list[1])
        day   = int( temp_1_list[2])
        hour  = int( temp_2_list[0])
        minute = int( temp_2_list[1])
        second = int( temp_2_list[2])
        time_struct = []
        time_struct.append(year)
        time_struct.append(month)
        time_struct.append(day)
        time_struct.append(hour)
        time_struct.append(minute)
        time_struct.append(second)
        time_struct.append(0)
        time_struct.append(0)
        time_struct.append(-1)
        input_data[i]["Time_Stamp"] = time.mktime(time_struct)
        input_data[i]["Year"] = int(temp_1_list[0])
        input_data[i]["Month"] = int( temp_1_list[1])
        input_data[i]["Day"]   = int( temp_1_list[2])
        input_data[i]["Hour"]  = int( temp_2_list[0])
        input_data[i]["Minute"] = int( temp_2_list[1])
        input_data[i]["Second"] = int( temp_2_list[2])
  

     return input_data



   def iterate_for_eto( self, daily_results):
     Ref_Time = daily_results[0]["Time_Stamp"]
     temp = 0
     temp_2 = 0
     returnValue = {}
     for i in range(0,len(daily_results)):
         if temp < float(daily_results[i]["dailyrainin"]) :
           temp = float(daily_results[i]["dailyrainin"])
         #print daily_results[i]["dailyrainin"],daily_results[i]["HourlyPrecipIn"]
         if float(daily_results[i]["HourlyPrecipIn"]) > 0:
          temp_2 = temp_2 + float(daily_results[i]["HourlyPrecipIn"])
         returnValue["daily_rain"] = float(daily_results[i]["dailyrainin"])
         if returnValue["daily_rain"] < 0 :
            returnValue["daily_rain"] = 0
     if temp < 0 :
         temp = 0

     #returnValue["daily_rain"] = temp_2/4
 
     wind_total = 0
     short_radiation = 0
     long_radiation =0
     windGust = 0
     windTime = 0
  
     day_of_year = time.localtime().tm_yday
     dr = 1+.033*math.cos(2*3.14159/365*day_of_year)
     delta = .409*math.sin(2*3.14159/365*day_of_year-1.39) 
     lat  = 3.14159/180*33.2
     omega = math.acos( -math.tan(lat)*math.tan(delta))
  
     ra = 24*60/3.14159*.0820*dr*((omega*math.sin(delta)*math.sin(lat))+(math.cos(delta)*math.cos(lat)*math.sin(omega) ))
     rso = (.75+2e-5*self.alt)*ra
     #print("rso",rso,ra,"omega",omega,"delta",delta)

     for i in daily_results    :
        #print "i",i
        i["Tc"] = (float(i["TemperatureF"])-32)*5/9
        tc = i["Tc"]
        if tc < 20:
            tc = 20

        i["v-m/sec"] = float(i["WindSpeedMPH"])*0.44704
        v = i["v-m/sec"]
        i["delta"] = 4098*(.6108*math.exp(tc*17.27/(tc+273.3)))/((tc+272.3)**2)
        i["dt"] = i["delta"]/(i["delta"]+self.gamma*(1+.34*v))
        i["pt"] = self.gamma/(i["delta"]+self.gamma*(1+.34*v))
        i["tt"] = (900/(tc+273))*v
        i["es"] = .6108*math.exp(17.27*tc/(tc+273.3))
        i["wind"] = i["pt"]*i["tt"]*i["es"]*(1-float(i["Humidity"])/100.)
        wind_total = wind_total +i["wind"]*( i["Time_Stamp"]-Ref_Time )/86400
        try:
           i["radiation"] = float(i["SolarRadiationWatts/m^2"])*.0864
        except:
	   i["radiation"] = 0
        short_radiation = short_radiation +i["radiation"]*(1-self.reflect)*i["dt"]*.408*( i["Time_Stamp"]-Ref_Time)/86400
        temp = 4.903e-9*((tc+273.3)**4)
    
        temp = temp*(.34-.14*math.sqrt(i["es"]*(1-float(i["Humidity"])/100.)))
        temp_2 = i["radiation"]/rso
        if temp_2 > 1.0:
          temp_2 = 1.0
        temp_1 = (1.35*temp_2-.35)
        if temp_1 < 0 :
          temp_1 = 0
          temp = temp*temp_1
        temp = temp*.408
     
        long_radiation = long_radiation + temp *( i["Time_Stamp"]-Ref_Time)/86400 
        if float(i["WindSpeedGustMPH"]) > windGust:
           windGust = float(i["WindSpeedGustMPH"])
           windTime = i["Time"]
           
        returnValue["Year"]  = i["Year"]
        returnValue["Month"] = i["Month"]
        returnValue["Day"]   = i["Day"]
        Ref_Time = i["Time_Stamp"]
   
     wind_total = wind_total/25.4
     short_radiation = short_radiation/25.4
     long_radiation = long_radiation/25.4
     returnValue["wind_total"] = wind_total
     returnValue["short_radiation"]=short_radiation
     returnValue["long_radiation"] = long_radiation
     returnValue["net_et"] = returnValue["wind_total"]+returnValue["short_radiation"] - returnValue["long_radiation"] - returnValue["daily_rain"] 
  
     returnValue["wind_gust"] = windGust
     returnValue["wind_gust_time_stamp"] = windTime
     returnValue["data_values"]= daily_results

     return returnValue


   def determine_et0( self,reference_time ):
      data_results = self.get_raw_data( reference_time   )
      if data_results == None:
          return
      #print "data_results",data_results
      data_results = self.parse_time_field( data_results[0] )
      et0 = self.iterate_for_eto(data_results)
      return et0


if __name__ == "__main__":
  alt = 2400 #feet
  sites = [ "MSRUC1", #SANTA ROSA PLATEAU CA US, Temecula, CA
            "MECSC1", #EL CARISO CA US, Lake Elsinore, CA
            "MCSPC1",  #CSS CASE SPRINGS CA US, Murrieta, CA
            "KF70"  #French field 
         ]
  eto = ETO(sites,alt)
  print(time.time())
  eto_data =    eto.determine_et0(time.time()  ) # get result for a day earlier
  #print eto_data
  if eto_data != None:
     print eto_data["net_et"]
  eto_data =    eto.determine_et0(time.time()-24*3600  ) # get result for a day earlier
  if eto_data != None:
    print eto_data["net_et"]





