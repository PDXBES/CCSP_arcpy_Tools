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


log_obj = utility.Logger(config.log_file)

log_obj.info("WFS to GDB - Process Started".format())

try:
    log_obj.info("WFS to GDB - getting layers".format())
    WFS_items = utility.get_item_list_from_dir(config.WFS_layers)

    for item in WFS_items:
        item_basename = utility.get_basename_no_extension(item)
        log_obj.info("WFS to GDB - copying {} to intermediate".format(item_basename))
        full_intermediate = os.path.join(config.WFS_intermediate, item_basename)
        working = arcpy.CopyFeatures_management(item, full_intermediate)

        log_obj.info("WFS to GDB - shortening fields as needed for {}".format(item_basename))
        field_names = utility.get_field_names_from_feature_class(working)
        for name in field_names:
            if name in config.rename_dict.keys():
                arcpy.management.AlterField(working, name, config.rename_dict[name])

        output_fc = os.path.join(config.GIS_TRANSFER10_GIS_sde_path, item_basename)
        #output_fc = os.path.join(config.BESGEORPT_sde_path, item_basename)
        #output_fc = os.path.join(r"\\besfile1\ccsp\Mapping\ArcPro_Projects\working\WFS_testing\TEST_output.gdb", item_basename)
        log_obj.info("WFS to GDB - saving to disk at - {}".format(output_fc))
        arcpy.CopyFeatures_management(working, output_fc)
        log_obj.info(" --- ")

    log_obj.info("WFS to GDB - Process Complete".format())

except Exception as e:
    arcpy.ExecuteError()
    log_obj.exception(str(sys.exc_info()[0]))
    log_obj.info("WFS to GDB Failed".format())
    pass