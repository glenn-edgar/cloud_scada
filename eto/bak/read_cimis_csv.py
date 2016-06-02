#
# This file reads the csv for the spacial csv
#
#
#
import os
import glob

class Station_Cimis():
   def __init__(self):  #no parameters
     pass

   def get_cimis_data( self ):
     os.chdir("email_data")
     files = glob.glob("L*.csv")  # get a list of files that match spatial cimis files
     lines = open(files[0]).readlines()
     keys = (lines[0].strip()).split(",")
     print("keys",keys)
     values = (lines[1].strip()).split(",")
     key1 = 'CIMIS ETo(in)'
     key2 = 'Precip(in)'
     cimis_data = {}
     try: 
       position1 = keys.index(key1)
       position2 = keys.index(key2)
       cimis_data["evap"]    = values[position1]
       cimis_data["precp"]   = values[position2]
     except:
       cimis_data["evap"]    = 0
       cimis_data["precp"]   = 0

 
     os.chdir("../" ) #return back to parnent dir
     return cimis_data
     

if __name__ == "__main__":
    x = Station_Cimis()
    cimis_data = x.get_cimis_data()
    print("station eto", cimis_data["evap"],cimis_data["precp"] )
