import os
import json
from datetime import datetime
import arcpy
import zipfile
import shutil
import logging
import sys
import logging.config

class Utility:
    def __init__(self, config):
        self.config = config

        self.city_standard_SRID = 2913

        #self.now_gdb_full_path_name = None

    #@staticmethod
    def datetime_now(self):
        return datetime.today().strftime('%Y%m%d%H%M%S')

    #@staticmethod
    #def date_today(date_object):
    #    return date_object.strftime('%Y%m%d')

    def ccsp_gdb_full_path_name(self):
        full_name = "CCSPToolsInput.gdb"
        full_ccsp_path = os.path.join(self.config.loader_output_base_folder, full_name)
        return full_ccsp_path

    def ccsp_gdb_noWB_full_path_name(self):
        full_name = "CCSPToolsInputNoWB.gdb"
        full_ccsp_path = os.path.join(self.config.loader_output_base_folder, full_name)
        return full_ccsp_path

    def intermediate_gdb_full_path_name(self):
        full_name = "data_load_intermediate.gdb"
        full_ccsp_path = os.path.join(self.config.loader_output_base_folder, full_name)
        return full_ccsp_path

    def now_ccsp_input_gdb_name(self):
        basename = "CCSPToolsInput_"
        datetime = self.datetime_now()
        extension = ".gdb"
        full_name = basename + datetime + extension
        return full_name

    def now_gdb_full_path_name(self):
        full_name = self.now_ccsp_input_gdb_name()
        full_path = os.path.join(self.config.archive_folder, full_name)
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
        #overwrites output of same name if exists
        split = os.path.basename(source_filename).split(".")
        new_name = split[0] + "." + split[1]
        new_dir = os.path.join(os.path.dirname(source_filename), new_name)
        self.delete_dir_if_exists(new_dir)
        os.mkdir(new_dir)
        with zipfile.ZipFile(source_filename) as zf:
            zf.extractall(new_dir)

    def zip(self, input_folder):
        #replaces .zip of same name if exists
        output_zipped_file = input_folder + ".zip"
        self.delete_file_if_exists(output_zipped_file)
        shutil.make_archive(input_folder, 'zip', input_folder)

    def zip_and_rename(self, input_folder, full_path_name):
        #output_folder = self.ccsp_gdb_full_path_name()
        output_zipped_file = full_path_name + ".zip"
        self.delete_file_if_exists(output_zipped_file)
        shutil.make_archive(full_path_name, 'zip', input_folder)

    def delete_dir_if_exists(self, input):
        if os.path.isdir(input):
            shutil.rmtree(input)
        else:
            pass

    def delete_file_if_exists(self, input):
        if os.path.isfile(input):
            os.remove(input)
        else:
            pass

    def rename_intermediate_gdb_to_input_gdb(self):
        os.rename(self.intermediate_gdb_full_path_name(), self.ccsp_gdb_full_path_name())

    def DME_master_hybrid_data_cleanup(self):
        feature_class_list = [self.config.DME_master_hybrid_sde_path]
        for feature_class in feature_class_list:
            try:
                arcpy.TruncateTable_management(feature_class)
            except:
                print("  unable to truncate, using Delete Rows")
                arcpy.DeleteRows_management(feature_class)

    def get_final_fc_list(self, gdb):
        arcpy.env.workspace = gdb
        full_list = []
        fc_list = arcpy.ListFeatureClasses()
        for fc in fc_list:
            full_list.append(fc)
        table_list = arcpy.ListTables()
        for table in table_list:
            full_list.append(table)
        return full_list

    def gdb_copy_name(self, input_gdb, output_dir):
        name = os.path.basename(input_gdb).split(".")[0]
        extention = os.path.basename(input_gdb).split(".")[1]
        new_full_path_name = os.path.join(output_dir, name + "_copy." + extention)
        return new_full_path_name

    def delete_feature_classes(self, gdb, feature_classes_list):
        # feature_classes is a list, can be list of one
        arcpy.env.workspace = gdb
        cws = arcpy.env.workspace

        fc_Delete = feature_classes_list

        for fc in fc_Delete:

            fc_path = os.path.join(cws, fc)
            if arcpy.Exists(fc_path):
                arcpy.Delete_management(fc_path)

        # https://stackoverflow.com/questions/6386698/how-to-write-to-a-file-using-the-logging-python-module
    def Logger(self, file_name):
        formatter = logging.Formatter(fmt='%(asctime)s %(module)s,line: %(lineno)d %(levelname)8s | %(message)s',
                                      datefmt='%Y/%m/%d %H:%M:%S')  # %I:%M:%S %p AM|PM format
        logging.basicConfig(filename='%s.log' % (file_name),
                            format='%(asctime)s %(module)s,line: %(lineno)d %(levelname)8s | %(message)s',
                            datefmt='%Y/%m/%d %H:%M:%S', filemode='a', level=logging.INFO)
        log_obj = logging.getLogger()
        log_obj.setLevel(logging.DEBUG)
        # log_obj = logging.getLogger().addHandler(logging.StreamHandler())

        # console printer
        screen_handler = logging.StreamHandler(stream=sys.stdout)  # stream=sys.stdout is similar to normal print
        screen_handler.setFormatter(formatter)
        logging.getLogger().addHandler(screen_handler)

        log_obj.info("Starting log session..")
        return log_obj
