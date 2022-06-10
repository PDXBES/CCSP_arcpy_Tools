import arcpy
import os
import sys
#import openpyxl
import json
from dataio import utility
from businessclasses import config
from datetime import datetime

#test_flag = "TEST"
test_flag = "PROD"

class DataLoad:
    def __init__(self):

        self.config = config.Config(test_flag)
        self.utility = utility.Utility(self.config)
        self.ccsp_gdb_full_path_name = self.utility.ccsp_gdb_full_path_name()

    def delete_existing_gdb(self, file_to_delete):
        if arcpy.Exists(file_to_delete):
            arcpy.Delete_management(file_to_delete)
        else:
            pass

    def create_gdb(self, gdb_full_path_name):
        self.delete_existing_gdb(gdb_full_path_name)
        arcpy.CreateFileGDB_management(self.config.loader_output_base_folder, os.path.basename(gdb_full_path_name))

    def create_input_dict_from_json_dict(self, data_source_file):
        input_dict = {}
        data = self.utility.create_dict_from_json(data_source_file)
        for key, value in data.items():
            input_dict[key] = value
        return input_dict

    def create_source_list_from_json_dict(self, appsettings_file):
        sourcelayer_list = []
        data = self.utility.create_dict_from_json(appsettings_file)
        LMdata = data["ETLServiceSettings"]["LayerMappings"] #returns list of dicts - gets all others
        #LLdata = data["ETLServiceSettings"]["LinksLayer"] #returns dict - gets bes_collection_system_master_hybrid_ccsp
        for item in LMdata:
            sourcelayer_list.append(item["SourceLayer"])
        #sourcelayer_list.append(LLdata["SourceLayer"])
        return sourcelayer_list

    def create_names_missing_from_source_list(self, appsettings_file, data_source_file):
        missing_source_names_list = []
        appsettings_list = set(self.create_source_list_from_json_dict(appsettings_file))
        input_dict = self.create_input_dict_from_json_dict(data_source_file)
        for item in appsettings_list:
            if item not in input_dict.keys():
                missing_source_names_list.append(item)
        return missing_source_names_list

    def create_names_missing_from_appsettings_list(self, appsettings_file, data_source_file):
        missing_appsettings_names_list = []
        appsettings_list = set(self.create_source_list_from_json_dict(appsettings_file))
        input_list = self.create_input_dict_from_json_dict(data_source_file)
        for item in input_list.keys():
            if item not in appsettings_list:
                missing_appsettings_names_list.append(item)
        return missing_appsettings_names_list

    def remove_extra_values(self, data_source_file, missing_appsettings_names_list):
        data_source_dict = self.create_input_dict_from_json_dict(data_source_file)
        for name in missing_appsettings_names_list:
            del data_source_dict[name]
        return data_source_dict

    def lists_identical(self, list1, list2):
        if sorted(list1) == sorted(list2):
            return True
        else:
            return False

    def input_source_names_identical(self, appsettings_file, data_source_file):
        # TODO - modify to test that all appsettings items are in source list (hard fail if no)
        # TODO - BUT allow process to continue with warning if there are extra values in source list
        # tests if names list associated with data sources are identical to those in appsettings
        appsettings_list = set(self.create_source_list_from_json_dict(appsettings_file))
        input_list = self.create_input_dict_from_json_dict(data_source_file)
        if self.lists_identical(appsettings_list, input_list.keys()):
            return True
        else:
            return False

    def copy_sources_to_gdb(self, data_dict, output_gdb):
        arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(self.utility.city_standard_SRID) #only applies on CopyFeatures
        if self.utility.valid_source_values(data_dict):
            try:
                print "Coping data sources to the gdb:"
                print "Input source count - " + str(len(data_dict))
                print "Input source list - " + str(data_dict.keys())
                for key, value in data_dict.items():
                    print "   Copying: " + str(key)
                    full_input_path = self.utility.source_formatter(value)
                    print "       Full input path: " + str(full_input_path)
                    print "           Exists: " + str(arcpy.Exists(full_input_path))
                    print "       Full output path: " + str(os.path.join(output_gdb, key))
                    if arcpy.Describe(self.utility.source_formatter(value)).dataType == 'FeatureClass':
                        arcpy.CopyFeatures_management(full_input_path, os.path.join(output_gdb, key))
                    elif arcpy.Describe(self.utility.source_formatter(value)).dataType == 'Table':
                        arcpy.Copy_management(full_input_path, os.path.join(output_gdb, key))
                        #arcpy.TableToTable_conversion(full_input_path, output_gdb, key)

            except:
                arcpy.ExecuteError()
        else:
            arcpy.AddError("Invalid data source(s)")
            arcpy.ExecuteError()
            raise Exception

    def load_data(self, appsettings_file, data_source_file):
        missing_from_source_names = self.create_names_missing_from_source_list(appsettings_file, data_source_file)
        if len(missing_from_source_names) == 0:

            missing_from_appsettings = self.create_names_missing_from_appsettings_list(appsettings_file,
                                                                                       data_source_file)
            if len(missing_from_appsettings) > 0:
                print "FYI - these entries are in the input source list but NOT IN the appsettings(required) list: "
                print "   " + str(missing_from_appsettings)
                print "      The extra entries will not be copied to the output gdb."
                filtered_dict = self.remove_extra_values(data_source_file,
                                                     missing_from_appsettings)
                self.copy_sources_to_gdb(filtered_dict, self.utility.intermediate_gdb_full_path_name())
            else:
                self.copy_sources_to_gdb(self.create_input_dict_from_json_dict(data_source_file), self.utility.intermediate_gdb_full_path_name())

        else:
            arcpy.AddError("No data will be copied")
            arcpy.AddError("All appsettings entries are required")
            arcpy.AddMessage("These entries are in the appsettings list but NOT IN the input source list: " + str(missing_from_source_names))
            arcpy.ExecuteError()
