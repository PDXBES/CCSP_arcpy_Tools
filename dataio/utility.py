import os
from datetime import datetime
import arcpy


def todays_gdb_name():
    basename = "PipXP_"
    today = datetime.today().strftime('%Y%m%d')
    extension = ".gdb"
    full_name = basename + today + extension
    return full_name

def todays_gdb_full_path_name(ETL_load_base_folder):
    full_name = todays_gdb_name()
    full_path = os.path.join(ETL_load_base_folder, full_name)
    return full_path

def source_formatter(sde_connections, source_string):
    if r"\\" in source_string:
        return source_string
    else:
        return os.path.join(sde_connections, source_string)

def valid_source_values(sde_connections, data_dict):
    valid = True
    for key, value in data_dict.iteritems():
        full_source = source_formatter(sde_connections, value)
        if arcpy.Exists(full_source) == False:
            print "Invalid source for: " + str(key)
            valid = False
    return valid