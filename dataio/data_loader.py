import arcpy
import os
import sys
import openpyxl
import json
from dataio import utility
from businessclasses import config
from datetime import datetime

test_flag = "TEST"

class DataLoad:
    def __init__(self):

        self.config = config.Config(test_flag)
        self.utility = utility.Utility(self.config)
        self.todays_gdb_full_path_name = self.utility.todays_gdb_full_path_name()

    def create_todays_gdb(self):
        if arcpy.Exists(self.todays_gdb_full_path_name):
            arcpy.Delete_management(self.todays_gdb_full_path_name)
        todays_gdb = self.utility.todays_ccsp_input_gdb_name()
        #print "Creating gdb " + str(todays_gdb)
        arcpy.CreateFileGDB_management(self.config.ETL_load_base_folder, todays_gdb)

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
        LLdata = data["ETLServiceSettings"]["LinksLayer"] #returns dict - gets bes_collection_system_master_hybrid_ccsp
        for item in LMdata:
            sourcelayer_list.append(item["SourceLayer"])
        sourcelayer_list.append(LLdata["SourceLayer"])
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

    def copy_sources(self, data_dict):
        arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(2913)
        if self.utility.valid_source_values(data_dict):
            print "Coping data sources to the gdb"
            for key, value in data_dict.items():
                print "   Copying: " + str(key)
                full_input_path = self.utility.source_formatter(value)
                #full_output_path = os.path.join(self.todays_gdb_full_path_name, key)
                #try:
                #    arcpy.Copy_management(full_input_path, full_output_path)
                try:
                    arcpy.FeatureClassToFeatureClass_conversion(full_input_path, self.todays_gdb_full_path_name, key)
                except Exception:
                    arcpy.TableToTable_conversion(full_input_path, self.todays_gdb_full_path_name, key)
        else:
            arcpy.AddError("Invalid data source(s)")
            arcpy.ExecuteError()
            raise Exception

    def load_data_to_gdb(self, appsettings_file, data_source_file):
        missing_from_source_names = self.create_names_missing_from_source_list(appsettings_file, data_source_file)
        if len(missing_from_source_names) == 0:
            try:
                self.create_todays_gdb()
                missing_from_appsettings = self.create_names_missing_from_appsettings_list(appsettings_file,
                                                                                           data_source_file)
                if len(missing_from_appsettings) > 0:
                    print "FYI: these entries are in the input source list but NOT IN the appsettings(required) list: "
                    print "   " + str(missing_from_appsettings)
                    print "      The extra entries will not be copied to the output gdb"
                    filtered_dict = self.remove_extra_values(data_source_file,
                                                         missing_from_appsettings)
                    self.copy_sources(filtered_dict)
                else:
                    self.copy_sources(self.create_input_dict_from_json_dict(data_source_file))

            except:
                if arcpy.Exists(self.todays_gdb_full_path_name):
                    arcpy.Delete_management(self.todays_gdb_full_path_name)
                arcpy.AddError("Data could not be loaded")
                arcpy.ExecuteError()
        else:
            print "No data will be copied"
            print "All appsettings entries are required"
            print "These entries are in the appsettings list but NOT IN the input source list: " + str(missing_from_source_names)

