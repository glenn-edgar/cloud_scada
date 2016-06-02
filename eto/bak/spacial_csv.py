#
# This file reads the csv for the spacial csv
#
#
#
import os
import glob

class Spatial_Cimis():
   def __init__(self):  #no parameters
     pass

   def get_cimis_data( self ):
     os.chdir("email_data")
     files = glob.glob("S*.csv")  # get a list of files that match spatial cimis files
     lines = open(files[0]).readlines()
     keys = (lines[0].strip()).split(",")
     values = (lines[1].strip()).split(",")
     key = "CIMIS ETo (in) (in/day)"
     try: 
       position = keys.index(key)
       cimis_data    = values[position]
     except:
         cimis_data = 0
     print "value" , cimis_data
     os.chdir("../" ) #return back to parnent dir
     return cimis_data

if __name__ == "__main__":
    x = Spatial_Cimis()
    print("spacial eto", x.get_cimis_data() )
