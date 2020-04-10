from dataio.data_loader import DataLoad
from datetime import datetime


data_load = DataLoad()

print "Starting process"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
data_load.load_data_to_gdb()
print "Process complete"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")