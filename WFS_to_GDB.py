from dataio.data_loader import DataLoad
import arcpy
from businessclasses import config
import os
import sys
import urllib

#################################################################################################
# process relies on set of lyrx files that have the WFS connection properties - (config.WFS_layers)
#################################################################################################

arcpy.env.overwriteOutput = True

test_flag = "PROD"

data_load = DataLoad()
utility = data_load.utility
config = config.Config(test_flag)

#arcpy.env.outputCoordinateSystem = utility.city_standard_SRID
# setting coordsys env seems to cause issues with JSONtoFeature + don't know if its really needed

lyrx_source = config.WFS_layers #PROD source
#lyrx_source = config.WFS_layers_QA #QA source

log_obj = utility.Logger(config.log_file)

log_obj.info("WFS to GDB - Process Started".format())


###  STANDARD ROUTE - WFS TO GDB  ###
log_obj.info("--- WFS to GDB - Standard Route ---")

log_obj.info("WFS to GDB - Getting layers from {}...".format(lyrx_source))
lyrx_list = utility.get_item_list_from_dir(lyrx_source)

layer_names = utility.get_layer_names_from_lyrx_files(lyrx_list)

log_obj.info("WFS to GDB - Process will be run for {} layers: ".format(len(layer_names)))
for name in layer_names:
    log_obj.info("   {}".format(name))

for lyrx in lyrx_list:
    try:
        item_basename = utility.get_lyrx_basename_no_extension(lyrx)

        log_obj.info("WFS to GDB - making feature layer from {}".format(item_basename))
        fl = utility.make_in_memory_feature_layer_from_lyrx(lyrx)

        log_obj.info("WFS to GDB - shortening field names as needed - ".format(item_basename))
        working_memory = utility.shorten_field_names(item_basename, fl)

        output_fc = os.path.join(config.GIS_TRANSFER10_GIS_sde_path, item_basename)
        # gdb = r"\\besfile1\ccsp\Mapping\Gdb\ESA_WFS_Prod_testing.gdb" #for testing, remove
        # output_fc = os.path.join(gdb, item_basename) # for testing, remove

        log_obj.info("WFS to GDB - saving to disk at - {}".format(output_fc))
        arcpy.CopyFeatures_management(working_memory, output_fc)

        arcpy.Delete_management(working_memory)

        log_obj.info("WFS to GDB - WFS straight to GDB complete for {}".format(item_basename))
        log_obj.info(" --- ")


    except Exception as e:
        arcpy.ExecuteError()
        log_obj.exception(str(sys.exc_info()[0]))
        log_obj.info("WFS to GDB error - {} did not write to gdb".format(lyrx))
        pass

log_obj.info("--- WFS to GDB - Standard Route Complete ---")

###  ALT ROUTE - WORKAROUND FOR PROBLEMATIC WFS - WFS TO JSON (THROUGH HTTP REQUEST) TO GDB ###
###  the standard route seems to hit a file size limit of 2Gb (best guess)
log_obj.info("--- WFS to GDB - Alt Route ---")

log_obj.info("WFS to GDB - Process will be run for: ")
for name in config.layer_names:
    log_obj.info(" ... {}".format(name))

headers = utility.create_headers()
#print(r.certs.where())

for layer_name in config.layer_names:
    try:
        log_obj.info("WFS to GDB - pulling WFS data for {}".format(layer_name))
        text = utility.request_json_as_text(config.wfs_url, headers, layer_name)

        log_obj.info("WFS to GDB - writing to json file")
        out_file = utility.write_text_as_json_file(layer_name, text)

        log_obj.info("WFS to GDB - converting json to feature class")
        # seeems like feature from this method must be written out to disk, cannot use in_memory version to proceed
        #in_memory_feature = arcpy.conversion.JSONToFeatures(out_file, 'in_memory/' + layer)
        feature = arcpy.conversion.JSONToFeatures(out_file,
                                                  os.path.join(config.WFS_intermediate_gdb, layer_name))

        log_obj.info("WFS to GDB - shortening field names as needed")
        working_memory = utility.shorten_field_names(layer_name, feature)

        # shape_len coming through as dec degrees or something so manually filling geom_length
        log_obj.info("WFS to GDB - adding geom_length")
        utility.add_and_populate_length_field(working_memory, 'geom_length')

        output_fc = os.path.join(config.GIS_TRANSFER10_GIS_sde_path, "ESA_" + layer_name)
        #gdb = r"\\besfile1\ccsp\Mapping\Gdb\ESA_WFS_Prod_testing.gdb" #for testing, remove
        #output_fc = os.path.join(gdb, layer) # for testing, remove

        log_obj.info("WFS to GDB - saving to disk at - {}".format(output_fc))
        arcpy.CopyFeatures_management(working_memory, output_fc)

        log_obj.info("WFS to GDB - WFS to JSON to GDB complete for {}".format(layer_name))

        log_obj.info("--- WFS to GDB - Alt Route Complete ---")

        log_obj.info("WFS to GDB - Full Process Complete".format())

    except urllib.error.HTTPError as e:
        message = layer_name + " Failed to write to file"
        log_obj.info(message)
        log_obj.info(e.reason)
        log_obj.info(str(sys.exc_info()[0]))

    except Exception as e:
        arcpy.ExecuteError()
        log_obj.exception(str(sys.exc_info()[0]))
        log_obj.info("WFS to GDB error - {} did not write to gdb".format(layer_name))







