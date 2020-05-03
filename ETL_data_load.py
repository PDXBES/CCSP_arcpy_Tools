from dataio.data_loader import DataLoad
from datetime import datetime


data_load = DataLoad()

input_appsettings_file = r"c:\temp\working\appsettings_example.json"

print "Process started"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
data_load.load_data_to_gdb(input_appsettings_file)
print "Process ended"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")