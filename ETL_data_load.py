from dataio.data_loader import DataLoad
from datetime import datetime
import arcpy
import DME_master_hybrid_citywide

appsettings_file = r"c:\temp\working\appsettings.json"
#appsettings_file = r"c:\temp\working\appsettings_REHAB_only.json"
#data_source_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\ETL_input_data_sources.json"
data_source_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\OLD_ETL_input_data_sources\ETL_input_data_sources_with_TEST1.json"

# ---------------------------------------------------------------

data_load = DataLoad()

print "ETL Data Loader - Process started"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")

try:
    print "Refreshing gdb - " + datetime.today().strftime("%H:%M:%S")
    data_load.create_gdb()

    print "Creating/ Loading DME master hybrid - " + datetime.today().strftime("%H:%M:%S")
    DME_master_hybrid_citywide.create_citywide_hybrid()

    print "Loading Data - " + datetime.today().strftime("%H:%M:%S")
    data_load.load_data(appsettings_file, data_source_file)

    print "Zipping gdb - " + datetime.today().strftime("%H:%M:%S")
    data_load.utility.zip(data_load.ccsp_gdb_full_path_name)

except:

    data_load.utility.delete_dir(data_load.ccsp_gdb_full_path_name)
    data_load.utility.delete_file(data_load.ccsp_gdb_full_path_name + ".zip")

    arcpy.AddError("Data could not be loaded")
    arcpy.ExecuteError()

print "Data Loader - Process ended"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")