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
        self.todays_gdb_full_path_name = self.utility.todays_gdb_full_path_name(datetime.today())

    def create_todays_gdb(self):
        if arcpy.Exists(self.todays_gdb_full_path_name):
            arcpy.AddError("gdb already exists")
            arcpy.ExecuteError()
            sys.exit("gdb already exists") #TODO - make sure this works as expected
        else:
            print "Creating gdb"
            arcpy.CreateFileGDB_management(self.config.ETL_load_base_folder, self.utility.todays_gdb_name(datetime.today()))

    def create_input_dict_from_json_dict(self):
        #print "Creating ETL input dict from json"
        input_dict = {}
        data = self.utility.create_dict_from_json(self.config.ETL_source_json)
        for key, value in data.items():
            input_dict[key] = value
        return input_dict

    def create_source_list_from_json_dict(self, input_appsettings_file):
        sourcelayer_list = []
        data = self.utility.create_dict_from_json(input_appsettings_file)
        LMdata = data["ETLServiceSettings"]["LayerMappings"] #returns list of dicts
        LLdata = data["ETLServiceSettings"]["LinksLayer"] #returns dict
        for item in LMdata:
            sourcelayer_list.append(item["SourceLayer"])
        sourcelayer_list.append(LLdata["SourceLayer"])
        return sourcelayer_list

    def create_missing_source_names_list(self, input_appsettings_file):
        missing_list = []
        appsettings_list = set(self.create_source_list_from_json_dict(input_appsettings_file))
        input_list = self.create_input_dict_from_json_dict()
        for item in appsettings_list:
            if item not in input_list.keys():
                missing_list.append(item)
        return missing_list

    def create_missing_appsettings_names_list(self, input_appsettings_file):
        missing_list = []
        appsettings_list = set(self.create_source_list_from_json_dict(input_appsettings_file))
        input_list = self.create_input_dict_from_json_dict()
        for item in input_list.keys():
            if item not in appsettings_list:
                missing_list.append(item)
        return missing_list

# TODO - test for if input source list has values not in appsettings list?

    def lists_identical(self, list1, list2):
        if sorted(list1) == sorted(list2):
            return True
        else:
            return False

    def input_source_names_identical(self, input_appsettings_file):
        # tests if names list associated with data sources are identical to those in appsettings
        appsettings_list = set(self.create_source_list_from_json_dict(input_appsettings_file))
        input_list = self.create_input_dict_from_json_dict()
        if self.lists_identical(appsettings_list, input_list.keys()):
            return True
        else:
            return False

    def copy_sources(self, data_dict):
        if self.utility.valid_source_values(data_dict):
            print "Coping data sources to the gdb"
            for key, value in data_dict.items():
                print "   Copying: " + str(key)
                full_input_path = self.utility.source_formatter(value)
                full_output_path = os.path.join(self.todays_gdb_full_path_name, key)
                try:
                    arcpy.Copy_management(full_input_path, full_output_path)
                except Exception:
                    arcpy.FeatureClassToFeatureClass_conversion(full_input_path, self.todays_gdb_full_path_name, key)
                except:
                    arcpy.TableToTable_conversion(full_input_path, self.todays_gdb_full_path_name, key)
        else:
            arcpy.AddError("Invalid data source(s)")
            arcpy.ExecuteError()
            raise Exception

# TODO - should both the appsettings and input source list be arguments?
    def load_data_to_gdb(self, input_appsettings_file):
        if self.input_source_names_identical(input_appsettings_file):
            try:
                self.create_todays_gdb()
                self.copy_sources(self.create_input_dict_from_json_dict())
            except:
                if arcpy.Exists(self.todays_gdb_full_path_name):
                    arcpy.Delete_management(self.todays_gdb_full_path_name)
                arcpy.AddError("Data could not be loaded")
                arcpy.ExecuteError()
        else:
            print "No data will be copied"
            print "The input source list does not match the appsettings list"
            missing_from_source_names = self.create_missing_source_names_list(input_appsettings_file)
            if len(missing_from_source_names) > 0:
                print "In appsettings but NOT IN input source names: " + str(missing_from_source_names)
            missing_from_appsettings = self.create_missing_appsettings_names_list(input_appsettings_file)
            if len(missing_from_appsettings) > 0:
                print "In input source names but NOT IN appsettings names: " + str(missing_from_appsettings)
