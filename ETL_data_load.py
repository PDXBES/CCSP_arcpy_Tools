from dataio.data_loader import DataLoad
from datetime import datetime


data_load = DataLoad()
appsettings_file = r"c:\temp\working\appsettings.json"
#appsettings_file = r"c:\temp\working\appsettings_REHAB_only.json"
#data_source_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\ETL_input_data_sources.json"
data_source_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\OLD_ETL_input_data_sources\ETL_input_data_sources_with_TEST1.json"

print "Data Loader - Process started"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")
data_load.load_data_to_gdb(appsettings_file, data_source_file)

#TODO - call DME_master_hybrid_citywide here to send its output to input_gdb

print "Data Loader - Process ended"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")