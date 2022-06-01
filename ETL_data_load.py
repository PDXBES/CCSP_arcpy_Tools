from dataio.data_loader import DataLoad
from datetime import datetime
import arcpy
import DME_master_hybrid_citywide
#from dataio.utility import Utility
from businessclasses import config
import os



# ---------------------------------------------------------------
test_flag = "PROD"

data_load = DataLoad()
utility = data_load.utility
config = config.Config(test_flag)

appsettings_file = os.path.join(config.loader_input_base_folder, "appsettings.json")
data_source_file = os.path.join(config.loader_input_base_folder, "ETL_input_data_sources.json")
#data_source_file = os.path.join(config.loader_input_base_folder, "ETL_input_data_sources - fake WB.json") #for manual run with fake WB - appsettings stays the same

log_obj = utility.Logger(config.log_file)

log_obj.info("ETL Data Loader - Process started".format())

try:

    log_obj.info("Creating intermediate gdb".format())
    data_load.create_gdb(utility.intermediate_gdb_full_path_name())

    log_obj.info("Loading Data".format())
    data_source_dict = data_load.create_input_dict_from_json_dict(data_source_file)
    print_string1 = "  Input sources that we are trying to load - " + str(data_source_dict.keys())
    print_string2 = "  Input source count - " + str(len(data_source_dict))
    log_obj.info(print_string1.format())
    log_obj.info(print_string2.format())
    data_load.load_data(appsettings_file, data_source_file)

    log_obj.info("GDB cleanup".format())
    log_obj.info("     delete existing load gdb".format())
    utility.delete_dir(utility.ccsp_gdb_full_path_name())
    log_obj.info("     rename intermediate to load gdb".format())
    utility.rename_intermediate_gdb_to_input_gdb()

    final_fc_list = utility.get_final_fc_list(utility.ccsp_gdb_full_path_name())
    log_obj.info("Final source count - " + str(len(final_fc_list)))

    log_obj.info("Zipping gdb".format())
    utility.zip(utility.ccsp_gdb_full_path_name())

except:

    utility.delete_dir(utility.intermediate_gdb_full_path_name())
    utility.delete_file(utility.intermediate_gdb_full_path_name() + ".zip")

    log_obj.info("DATA COULD NOT BE LOADED".format())
    arcpy.ExecuteError()

log_obj.info("Data Loader - Process ended".format())
