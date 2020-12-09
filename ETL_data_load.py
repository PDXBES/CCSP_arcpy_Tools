from dataio.data_loader import DataLoad
from datetime import datetime
import arcpy
import DME_master_hybrid_citywide
#from dataio.utility import Utility
#from businessclasses import config

appsettings_file = r"c:\temp\working\appsettings.json"
#appsettings_file = r"c:\temp\working\appsettings_REHAB_only.json"
data_source_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\CCSP_Tools_Input\ETL_input_data_sources.json"
#data_source_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\OLD_ETL_input_data_sources\ETL_input_data_sources_with_TEST1.json"

# ---------------------------------------------------------------
test_flag = "PROD"

data_load = DataLoad()
utility = data_load.utility

print "ETL Data Loader - Process started"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")

try:

    print "DME master hybrid cleanup - " + datetime.today().strftime("%H:%M:%S")
    utility.DME_master_hybrid_data_cleanup()

    print "Creating DME master hybrid - " + datetime.today().strftime("%H:%M:%S")
    DME_master_hybrid_citywide.create_citywide_hybrid()

    print "Creating intermediate gdb - " + datetime.today().strftime("%H:%M:%S")
    data_load.create_gdb(utility.intermediate_gdb_full_path_name())

    print "Loading Data - " + datetime.today().strftime("%H:%M:%S")
    data_load.load_data(appsettings_file, data_source_file)

    #Q: want to rename load.gdb instead of del? - that would give access to 1 version prior

    print "GDB cleanup - " + datetime.today().strftime("%H:%M:%S")
    print "     delete existing load gdb"
    utility.delete_dir(utility.ccsp_gdb_full_path_name())
    print "     rename intermediate to load gdb"
    utility.rename_intermediate_gdb_to_input_gdb()

    print "Zipping gdb - " + datetime.today().strftime("%H:%M:%S")
    utility.zip(utility.ccsp_gdb_full_path_name())

except:

    utility.delete_dir(utility.intermediate_gdb_full_path_name())
    utility.delete_file(utility.intermediate_gdb_full_path_name() + ".zip")

    arcpy.AddError("Data could not be loaded")
    arcpy.ExecuteError()

print "Data Loader - Process ended"
print datetime.today().strftime("%m/%d/%Y, %H:%M:%S")