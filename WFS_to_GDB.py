from dataio.data_loader import DataLoad
import arcpy
from businessclasses import config
import os
import sys

#################################################################################################
# process relies on set of lyrx files that have the WFS connection properties (config.WFS_layers)
#################################################################################################

arcpy.env.overwriteOutput = True

test_flag = "PROD"

data_load = DataLoad()
utility = data_load.utility
config = config.Config(test_flag)

arcpy.env.outputCoordinateSystem = 2913

log_obj = utility.Logger(config.log_file)

log_obj.info("WFS to GDB - Process Started".format())

try:
    log_obj.info("WFS to GDB - getting layers".format())
    #WFS_items = utility.get_item_list_from_dir(config.WFS_layers)
    WFS_items = utility.get_item_list_from_dir(config.WFS_layers_QA) # for testing, remove

    layer_names = []
    for item in WFS_items:
        layer_names.append(os.path.basename(item))

    log_obj.info("WFS to GDB - Running process for : ")
    for name in layer_names:
        log_obj.info("   {}".format(name))

    for item in WFS_items:
        item_basename = utility.get_basename_no_extension(item)
        log_obj.info("WFS to GDB - copying {} to intermediate".format(item_basename))

        # must be 'in_memory' (not 'memory') for AlterField to work
        in_memory_intermediate = os.path.join("in_memory", item_basename)
        working_in_memory = arcpy.CopyFeatures_management(item, in_memory_intermediate)

        log_obj.info("WFS to GDB - shortening fields as needed for {}".format(item_basename))
        field_names = utility.get_field_names_from_feature_class(working_in_memory)
        for name in field_names:
            if name in config.rename_dict.keys():
                arcpy.management.AlterField(working_in_memory, name, config.rename_dict[name])

        # must be in 'memory' (not 'in_memory') for Copy to GIS_TRANSFER10 to work
        memory_intermediate = os.path.join("memory", item_basename)
        working_memory = arcpy.CopyFeatures_management(working_in_memory, memory_intermediate)

        # output_fc = os.path.join(config.GIS_TRANSFER10_GIS_sde_path, item_basename)
        gdb = r"\\besfile1\ccsp\Mapping\Gdb\ESA_WFS_QA_testing.gdb" #for testing, remove
        output_fc = os.path.join(gdb, item_basename) # for testing, remove

        log_obj.info("WFS to GDB - saving to disk at - {}".format(output_fc))
        arcpy.CopyFeatures_management(working_memory, output_fc)
        arcpy.Delete_management(working_in_memory)
        arcpy.Delete_management(working_memory)
        log_obj.info(" --- ")

    log_obj.info("WFS to GDB - Process Complete".format())

except Exception as e:
    arcpy.ExecuteError()
    log_obj.exception(str(sys.exc_info()[0]))
    log_obj.info("WFS to GDB Failed".format())
    pass