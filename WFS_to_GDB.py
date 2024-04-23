from dataio.data_loader import DataLoad
import arcpy
# from businessclasses import config
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
# config = config.Config(test_flag)

arcpy.env.outputCoordinateSystem = utility.city_standard_SRID
# setting coordsys env seems to cause issues with JSONtoFeature but it is needed for the standard route

# -----------------------------------------------------------------------------------------------------
lyrx_source = data_load.config.WFS_layers #PROD source
#lyrx_source = config.WFS_layers_testing # just a few testing copies here
#lyrx_source = config.WFS_layers_QA #QA source

output_gdb = data_load.config.GIS_TRANSFER10_GIS_sde_path
#output_gdb = r"\\besfile1\ccsp\Mapping\Gdb\ESA_WFS_Prod_testing.gdb" #for testing, eventually remove
# -----------------------------------------------------------------------------------------------------


log_obj = utility.Logger(data_load.config.log_file)

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

        log_obj.info("WFS to GDB - shortening field names as needed".format(item_basename))
        working_memory = utility.shorten_field_names(item_basename, fl)

        log_obj.info("WFS to GDB - adding geometry value")
        geom_value = utility.get_geomattribute_value_by_type(utility.get_shape_type(working_memory))
        utility.add_and_populate_geometry_field(working_memory, geom_value)

        if item_basename in data_load.config.CCSP_subset_layers:
            log_obj.info("WFS to GDB - filtering Dashboard results to CCSP areas")
            target_key_field = data_load.config.CCSP_subset_layers[item_basename][0]
            exclusion_fc = data_load.config.CCSP_subset_layers[item_basename][1]
            exclusion_key_field = data_load.config.CCSP_subset_layers[item_basename][2]

            value_list = utility.get_field_value_list(exclusion_fc, exclusion_key_field)

            list_as_string = str(tuple(value_list))
            exp = "{} in {}".format(target_key_field, list_as_string)
            log_obj.info("Record count before filter: " + str(arcpy.GetCount_management(working_memory)))
            fl_for_export = arcpy.MakeFeatureLayer_management(working_memory, r"in_memory/fl_for_export", exp)
            log_obj.info("Record count after filter: " + str(arcpy.GetCount_management(fl_for_export)))

            output_fc = os.path.join(output_gdb, item_basename)

            log_obj.info("WFS to GDB - saving to disk at - {}".format(output_fc))
            arcpy.CopyFeatures_management(fl_for_export, output_fc)

            arcpy.Delete_management(fl)
            arcpy.Delete_management(working_memory)
            arcpy.Delete_management(fl_for_export)

        else:
            output_fc = os.path.join(output_gdb, item_basename)

            log_obj.info("WFS to GDB - saving to disk at - {}".format(output_fc))
            arcpy.CopyFeatures_management(working_memory, output_fc)

            arcpy.Delete_management(fl)
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
###  the standard route is problematic for COF, also on besapp4 the lyrx file credentials for WFS don't seem to work
# log_obj.info("--- WFS to JSON to GDB (Alt Route) ---")
#
# log_obj.info("WFS to JSON to GDB - Process will be run for {} layers: ".format(len(data_load.config.layer_names)))
# for name in data_load.config.layer_names:
#     log_obj.info(" ... {}".format(name))
#
# headers = utility.create_headers()
# #print(r.certs.where())
#
# for layer_name in data_load.config.layer_names:
#     try:
#         log_obj.info("WFS to JSON to GDB - pulling WFS data for {}".format(layer_name))
#         text = utility.request_json_as_text(data_load.config.wfs_url, headers, layer_name)
#
#         log_obj.info("WFS to JSON to GDB - writing to json file")
#         out_file = utility.write_text_as_json_file(layer_name, text)
#
#         log_obj.info("WFS to JSON to GDB - converting json to feature class")
#         # seems like feature from this method must be written out to disk, cannot use in_memory version to proceed
#         #in_memory_feature = arcpy.conversion.JSONToFeatures(out_file, 'in_memory/' + layer)
#         feature = arcpy.conversion.JSONToFeatures(out_file,
#                                                   os.path.join(data_load.config.WFS_intermediate_gdb, layer_name))
#
#         log_obj.info("WFS to GDB - shortening field names as needed")
#         working_memory = utility.shorten_field_names(layer_name, feature)
#
#         # shape_len coming through as dec degrees or something so manually filling geom values
#         log_obj.info("WFS to JSON to GDB - adding geometry value (if needed)")
#         geom_value = utility.get_geomattribute_value_by_type(utility.get_shape_type(working_memory))
#         utility.add_and_populate_geometry_field(working_memory, geom_value)
#
#         output_fc = os.path.join(output_gdb, "ESA_" + layer_name)
#
#         log_obj.info("WFS to JSON to GDB - saving to disk at - {}".format(output_fc))
#         #arcpy.CopyFeatures_management(working_memory, output_fc)
#          arcpy.FeatureClassToFeatureClass_conversion(working_memory,
#                                                      data_load.config.GIS_TRANSFER10_GIS_sde_path
#                                                      "ESA_" + layer_name)
#
#         log_obj.info("WFS to JSON to GDB - WFS to JSON to GDB complete for {}".format(layer_name))
#         log_obj.info(" ----------------------------------------------------- ")
#
#
#     except urllib.error.HTTPError as e:
#         message = layer_name + " Failed to write to file"
#         log_obj.info(message)
#         log_obj.info(e.reason)
#         log_obj.info(str(sys.exc_info()[0]))
#
#     except Exception as e:
#         arcpy.ExecuteError()
#         log_obj.exception(str(sys.exc_info()[0]))
#         log_obj.info("WFS to JSON to GDB error - {} did not write to gdb".format(layer_name))
#
# log_obj.info("--- WFS to JSON to GDB (Alt Route) Complete ---")
log_obj.info("WFS to GDB - Full Process Complete".format())








