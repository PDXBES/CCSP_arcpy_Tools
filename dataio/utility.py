import os
import json
from datetime import datetime
import arcpy


class Utility:
    def __init__(self, config):
        self.config = config

    #@staticmethod
    #def date_today(self):
    #    return datetime.today().strftime('%Y%m%d')

    @staticmethod
    def date_today(date_object):
        return date_object.strftime('%Y%m%d')

    def todays_gdb_name(self, date_object):
        basename = "CCSPToolsInput_"
        today = self.date_today(date_object)
        extension = ".gdb"
        full_name = basename + today + extension
        return full_name

    def todays_gdb_full_path_name(self, date_object):
        full_name = self.todays_gdb_name(date_object)
        full_path = os.path.join(self.config.ETL_load_base_folder, full_name)
        return full_path

    def source_formatter(self, source_string):
        if r"\\" in source_string:
            return source_string
        else:
            return os.path.join(self.config.sde_connections, source_string)

    def valid_source_values(self, data_dict):
        valid = True
        for key, value in data_dict.items():
            full_source = self.source_formatter(value)
            if not arcpy.Exists(full_source):
                print "Check the data source file - Invalid source for: " + str(key)
                valid = False
        return valid

    def create_dict_from_json(self, input_json_file):
        if arcpy.Exists(input_json_file):
            with open(input_json_file) as json_file:
                data = json.load(json_file)
            return data
        else:
            arcpy.AddError("Invalid json source")
            arcpy.ExecuteError()
            raise Exception