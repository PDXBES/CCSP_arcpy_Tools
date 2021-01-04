import os
import sys
import arcpy
from datetime import datetime

def gpkg_to_gdb(input_gpkg_path):
    if os.path.isfile(input_gpkg_path):
        print("Starting gpkg to gdb conversion process " + datetime.today().strftime("%H:%M:%S"))
        output_gdb_name = os.path.basename(input_gpkg_path).split(".")[0] + ".gdb"
        basename = os.path.basename(input_gpkg_path)
        full_output_gdb_path = os.path.join(os.path.dirname(input_gpkg_path), output_gdb_name)
        if os.path.isfile(full_output_gdb_path):
            sys.exit("Output gdb already exists")
        arcpy.management.CreateFileGDB(os.path.dirname(input_gpkg_path), output_gdb_name)
        arcpy.env.workspace = input_gpkg_path
        fc_list = arcpy.ListFeatureClasses()
        arcpy.conversion.FeatureClassToGeodatabase(fc_list, full_output_gdb_path)
        print("Conversion process complete " + datetime.today().strftime("%H:%M:%S"))
    else:
        print("Invalid gpkg path")
        pass
    
if __name__ == "__main__":
    gpkg_to_gdb(sys.argv[1])