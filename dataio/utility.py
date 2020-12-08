import os
import json
from datetime import datetime
import arcpy
import zipfile
import shutil

class Utility:
    def __init__(self, config):
        self.config = config

        self.standard_SRID = 2913

    #@staticmethod
    #def date_today(self):
    #    return datetime.today().strftime('%Y%m%d')


    @staticmethod
    def date_today(date_object):
        return date_object.strftime('%Y%m%d')

    def ccsp_gdb_name(self):
        basename = "CCSPToolsInput"
        #today = self.date_today(date_object)
        extension = ".gdb"
        full_name = basename + extension
        return full_name

    def ccsp_gdb_full_path_name(self):
        full_name = self.ccsp_gdb_name()
        full_path = os.path.join(self.config.ETL_load_base_folder, full_name)
        return full_path

    def todays_ccsp_input_gdb_name(self, date_object):
        basename = "CCSPToolsInput_"
        today = self.date_today(date_object)
        extension = ".gdb"
        full_name = basename + today + extension
        return full_name

    def todays_gdb_full_path_name(self, date_object):
        full_name = self.todays_ccsp_input_gdb_name(date_object)
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

    def unzip(self, source_filename):
        #overwrites output of same name if exixts
        split = os.path.basename(source_filename).split(".")
        new_name = split[0] + "." + split[1]
        new_dir = os.path.join(os.path.dirname(source_filename), new_name)
        self.delete_dir(new_dir)
        os.mkdir(new_dir)
        with zipfile.ZipFile(source_filename) as zf:
            zf.extractall(new_dir)

    def zip(self, input_folder):
        arcpy.AddMessage("Creating zipped folder")
        #overwrites .zip of same name if exists
        new_zipped_file = input_folder + ".zip"
        self.delete_file(new_zipped_file)
        shutil.make_archive(input_folder, 'zip', input_folder)

    def delete_dir(self, input):
        if os.path.isdir(input):
            shutil.rmtree(input)

    def delete_file(self, input):
        if os.path.isfile(input):
            os.remove(input)

    def rename_gdb(self):
        #os.rename(#intermediate_gdb, self.ccsp_gdb_full_path_name)
        pass

    def DME_master_hybrid_data_cleanup(self):
        feature_class_list = [self.config.DME_master_hybrid_sde_path]
        for feature_class in feature_class_list:
            try:
                arcpy.TruncateTable_management(feature_class)
            except:
                print("  unable to truncate, using Delete Rows")
                arcpy.DeleteRows_management(feature_class)
