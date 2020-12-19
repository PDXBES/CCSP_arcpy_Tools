from dataio.data_loader import DataLoad
from datetime import datetime
import arcpy
import DME_master_hybrid_citywide
#from dataio.utility import Utility
from businessclasses import config

#appsettings_file = r"c:\temp\working\appsettings.json"
appsettings_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\CCSP_Tools_Input\appsettings.json"
data_source_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\CCSP_Tools_Input\ETL_input_data_sources.json"

# ---------------------------------------------------------------
test_flag = "PROD"

data_load = DataLoad()
utility = data_load.utility
config = config.Config(test_flag)

log_obj = utility.Logger(config.log_file)

log_obj.info("ETL Data Loader - Process started".format())

try:

    log_obj.info("DME master hybrid cleanup".format())
    utility.DME_master_hybrid_data_cleanup()

    log_obj.info("Creating DME master hybrid".format())
    DME_master_hybrid_citywide.create_citywide_hybrid()

    log_obj.info("Creating intermediate gdb".format())
    data_load.create_gdb(utility.intermediate_gdb_full_path_name())

    log_obj.info("Loading Data".format())
    data_load.load_data(appsettings_file, data_source_file)

    log_obj.info("GDB cleanup".format())
    log_obj.info("     delete existing load gdb".format())
    utility.delete_dir(utility.ccsp_gdb_full_path_name())
    log_obj.info("     rename intermediate to load gdb".format())
    utility.rename_intermediate_gdb_to_input_gdb()

    log_obj.info("Zipping gdb".format())
    utility.zip(utility.ccsp_gdb_full_path_name())

except:

    utility.delete_dir(utility.intermediate_gdb_full_path_name())
    utility.delete_file(utility.intermediate_gdb_full_path_name() + ".zip")

    log_obj.info("Data could not be loaded".format())
    arcpy.ExecuteError()

log_obj.info("Data Loader - Process ended".format())
