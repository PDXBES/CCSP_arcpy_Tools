from dataio.data_loader import DataLoad
import arcpy
from businessclasses import config
import os
import sys

#################################################################################################
# process relies on set of lyrx files that have the WFS connection properties - (config.WFS_layers)
#################################################################################################

arcpy.env.overwriteOutput = True

test_flag = "PROD"

data_load = DataLoad()
utility = data_load.utility
config = config.Config(test_flag)

arcpy.env.outputCoordinateSystem = utility.city_standard_SRID
lyrx_source = config.WFS_layers #PROD source
#lyrx_source = config.WFS_layers_QA #QA source

log_obj = utility.Logger(config.log_file)

log_obj.info("WFS to GDB - Process Started".format())




try:
    log_obj.info("WFS to GDB - Getting layers from {}...".format(lyrx_source))
    lyrx_list = utility.get_item_list_from_dir(lyrx_source)

    layer_names = []
    for lyrx in lyrx_list:
        layer_names.append(os.path.basename(lyrx))

    log_obj.info("WFS to GDB - Running process for {} layers: ".format(len(layer_names)))
    for name in layer_names:
        log_obj.info("   {}".format(name))

    for lyrx in lyrx_list:
        item_basename = utility.get_lyrx_basename_no_extension(lyrx)

        log_obj.info("WFS to GDB - making feature layer from {}".format(item_basename))
        fl = utility.make_in_memory_feature_layer_from_lyrx(lyrx)

        log_obj.info("WFS to GDB - copying {} feature layer to intermediate (memory space)".format(item_basename))
        working_memory = utility.shorten_field_names(item_basename, fl)

        output_fc = os.path.join(config.GIS_TRANSFER10_GIS_sde_path, item_basename)
        # gdb = r"\\besfile1\ccsp\Mapping\Gdb\ESA_WFS_Prod_testing.gdb" #for testing, remove
        # output_fc = os.path.join(gdb, item_basename) # for testing, remove

        log_obj.info("WFS to GDB - saving to disk at - {}".format(output_fc))
        arcpy.CopyFeatures_management(working_memory, output_fc)

        arcpy.Delete_management(working_memory)
        log_obj.info(" --- ")

    log_obj.info("WFS to GDB - Process Complete".format())

except Exception as e:
    arcpy.ExecuteError()
    log_obj.exception(str(sys.exc_info()[0]))
    log_obj.info("WFS to GDB Failed".format())
    pass