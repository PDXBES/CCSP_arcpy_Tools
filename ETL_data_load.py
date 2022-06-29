from dataio.data_loader import DataLoad
from datetime import datetime
import arcpy
import DME_master_hybrid_citywide
#from dataio.utility import Utility
from businessclasses import config
import os
import sys


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

utility.now_gdb_full_path_name()
#now_gdb_full_path_name = data_load.now_gdb_full_path_name

try:

    log_obj.info("Creating gdb for this run (datetime stamped)".format())

    data_load.create_gdb(data_load.now_gdb_full_path_name)

    log_obj.info("Loading Data".format())
    data_load.load_data(appsettings_file, data_source_file)

    final_fc_list = utility.get_final_fc_list(data_load.now_gdb_full_path_name)
    log_obj.info("Final source count - " + str(len(final_fc_list)))

    log_obj.info("Creating zipped CCSPToolsInput.gdb (overwrite existing)".format())
    utility.zip_and_rename(data_load.now_gdb_full_path_name, utility.ccsp_gdb_full_path_name())

    log_obj.info("Creating zipped CCSPToolsInputNoWB.gdb (overwrite existing)".format())
    gdb_copy_name = utility.gdb_copy_name(data_load.now_gdb_full_path_name, config.archive_folder)
    log_obj.info("     creating result copy in Archive".format())
    arcpy.Copy_management(data_load.now_gdb_full_path_name, gdb_copy_name)
    log_obj.info("     deleting WB data from copy".format())
    utility.delete_feature_classes(gdb_copy_name, ["pressure_Mains"])
    log_obj.info("     saving out zipped version".format())
    utility.zip_and_rename(gdb_copy_name, utility.ccsp_gdb_noWB_full_path_name())
    log_obj.info("     deleting copy".format())
    arcpy.Delete_management(gdb_copy_name)

    log_obj.info("Archiving run result".format())
    log_obj.info("     create zipped version of run result".format())
    utility.zip(data_load.now_gdb_full_path_name)
    log_obj.info("     delete run result".format())
    utility.delete_dir_if_exists(data_load.now_gdb_full_path_name)

except Exception as e:

    utility.delete_dir_if_exists(data_load.now_gdb_full_path_name)
    #utility.delete_file_if_exists(utility.ccsp_gdb_full_path_name() + ".zip")

    log_obj.info("DATA COULD NOT BE LOADED".format())
    arcpy.ExecuteError()
    log_obj.exception(str(sys.exc_info()[0]))

log_obj.info("Data Loader - Process ended".format())
 