import os
import json
from datetime import datetime
import arcpy
import zipfile
import shutil
import logging
import sys
import logging.config
from base64 import b64encode
import requests

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
                print("Check the data source file - Invalid source for: " + str(key))
                valid = False
        return valid

    def create_dict_from_json(self, input_json_file):
        if os.path.exists(input_json_file):
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

    # def zip_gdb(self, input_folder):
    #     #replaces .zip of same name if exists
    #     output_zipped_file = input_folder + ".zip"
    #     self.delete_file_if_exists(output_zipped_file)
    #     shutil.make_archive(input_folder, 'zip', input_folder)
    #
    # def zip_and_rename_gdb(self, input_folder, full_path_name):
    #     #output_folder = self.ccsp_gdb_full_path_name()
    #     output_zipped_file = full_path_name + ".zip"
    #     self.delete_file_if_exists(output_zipped_file)
    #     shutil.make_archive(full_path_name, 'zip', input_folder)

    def zip_gdb(self, inputGDB):
        gdbFile = str(inputGDB)
        # output_folder = self.ccsp_gdb_full_path_name()
        outFile = gdbFile + '.zip'
        self.delete_file_if_exists(outFile)
        gdbName = os.path.basename(gdbFile)
        with zipfile.ZipFile(outFile, mode='w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as myzip:
            for f in os.listdir(gdbFile):
                if f[-5:] != '.lock':
                    myzip.write(os.path.join(gdbFile, f), os.path.basename(f))

    def zip_and_rename_gdb(self, inputGDB, outputGDB):
        gdbFile = str(inputGDB)
        # output_folder = self.ccsp_gdb_full_path_name()
        outFile = outputGDB + '.zip'
        self.delete_file_if_exists(outFile)
        gdbName = os.path.basename(gdbFile)
        with zipfile.ZipFile(outFile, mode='w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as myzip:
            for f in os.listdir(gdbFile):
                if f[-5:] != '.lock':
                    myzip.write(os.path.join(gdbFile, f), os.path.basename(f))

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

    def datetime_print(self, message):
        print(datetime.now().strftime("%Y/%m/%d %H:%M:%S") + " " + message)

    def get_item_list_from_dir(self, dir):
        item_list = []
        for item in os.listdir(dir):
            item_list.append(os.path.join(dir, item))
        return item_list

    def get_field_names_from_feature_class(self, feature_class):
        field_names = []
        fields = arcpy.ListFields(feature_class)
        for field in fields:
            field_names.append(field.name)
        return field_names

    def get_lyrx_basename_no_extension(self, full_path):
        basename = os.path.basename(full_path)
        result = basename.split('.')[0]
        return result

    def get_first_half_of_list(self, list):
        half = len(list) >> 1
        firsthalf = list[:half]
        return firsthalf

    def get_second_half_of_list(self, list):
        half = len(list) >> 1
        secondhalf = list[half:]
        return secondhalf

    def get_half_of_list(self, list, which_half): #which_half = 'first' or 'second'
        if which_half == 'first':
            half = self.get_first_half_of_list(list)
            return half
        elif which_half == 'second':
            half = self.get_second_half_of_list(list)
            return half

    def extract_dict_by_key(self, key, json_as_dict):
        extracted_dict = {}
        extracted_dict[key] = json_as_dict[key]
        return extracted_dict

    def split_json_as_dict(self, json_as_dict): #returns a list with each of the split json dicts
        type_dict = self.extract_dict_by_key('type', json_as_dict)

        features_dict = self.extract_dict_by_key('features', json_as_dict)
        features_key_value = list(features_dict.keys())[0]

        first_features_dict = {}
        first_features_dict[features_key_value] = self.get_half_of_list(features_dict[features_key_value], 'first')
        first_compiled_dict = {**type_dict, **first_features_dict}

        second_features_dict = {}
        second_features_dict[features_key_value] = self.get_half_of_list(features_dict[features_key_value], 'second')
        second_compiled_dict = {**type_dict, **second_features_dict}

        split_list = [first_compiled_dict, second_compiled_dict]

        return split_list

    def write_json_to_disk(self, json_as_dict, outfile):
        file = open(outfile, "w")
        json.dump(json_as_dict, file, indent=4)

    def get_count_of_features_in_json_dict(self, json_as_dict):
        count = len(json_as_dict['features'])
        return count

    def get_cred_values(self, cred_file):
        reader = open(cred_file, "r")
        readlines = reader.readlines()
        creds = []
        for line in readlines:
            creds.append(line.strip('\n'))
        return creds

    def basic_auth(self, username, password):
        token = b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
        return f'Basic {token}'

    def make_in_memory_feature_layer_from_lyrx(self, layer_file):
        item_basename = self.get_lyrx_basename_no_extension(layer_file)
        fl_intermediate = os.path.join("in_memory", item_basename + "_fl")
        fl = arcpy.MakeFeatureLayer_management(layer_file, fl_intermediate)
        return fl

    def make_in_memory_feature_layer(self, feature, name):
        fl_intermediate = os.path.join("in_memory", name + "_fl")
        fl = arcpy.MakeFeatureLayer_management(feature, fl_intermediate)
        return fl

    def make_in_memory_object(self, feature_layer, item_basename):
        in_memory_intermediate = os.path.join("in_memory", item_basename)
        working_in_memory = arcpy.CopyFeatures_management(feature_layer, in_memory_intermediate)
        return working_in_memory

    def make_memory_object_from_in_memory_object(self, item_basename, working_in_memory):
        memory_intermediate = os.path.join("memory", item_basename)
        working_memory = arcpy.CopyFeatures_management(working_in_memory, memory_intermediate)
        return working_memory

    def rename_fields_from_dict(self, working_in_memory, renaming_dict):
        field_names = self.get_field_names_from_feature_class(working_in_memory)
        for name in field_names:
            if name in renaming_dict.keys():
                arcpy.management.AlterField(working_in_memory, name, renaming_dict[name])

    def shorten_field_names(self, item_basename, feature_layer):
        # must be 'in_memory' (not 'memory') for AlterField to work
        working_in_memory = self.make_in_memory_object(feature_layer, item_basename)
        #self.datetime_print("shortening fields as needed for {}".format(item_basename))
        self.rename_fields_from_dict(working_in_memory, self.config.rename_dict)
        # must be in 'memory' (not 'in_memory') for Copy to GIS_TRANSFER10 to work
        working_memory = self.make_memory_object_from_in_memory_object(item_basename, working_in_memory)
        arcpy.Delete_management(working_in_memory)
        return working_memory

    def list_field_names(self, input_fc):
        field_names = []
        fields = arcpy.ListFields(input_fc)
        for field in fields:
            field_names.append(field.name)
        return field_names

    def add_and_populate_geometry_field(self, feature_class, geom_value):
        if geom_value == 'Other':
            pass
        else:
            field_name = "geom_" + geom_value
            self.add_field_if_needed(feature_class, field_name, 'DOUBLE', '', 4)
            arcpy.management.CalculateGeometryAttributes(feature_class,
                                                         "{} {}".format(field_name, geom_value),
                                                         '',
                                                         '',
                                                         2913)

    def get_shape_type(self, fc):
        desc = arcpy.Describe(fc)
        shape_type = desc.shapeType
        return shape_type

    def get_geomattribute_value_by_type(self, shape_type):
        if shape_type == 'Polyline':
            return 'LENGTH'
        elif shape_type == 'Polygon':
            return 'AREA'
        else:
            return 'Other'

    def add_field_if_needed(self, input_fc, field_to_add, field_type, precision=None, scale=None, length=None):
        field_names = self.list_field_names(input_fc)
        if field_to_add not in field_names:
            arcpy.AddField_management(input_fc, field_to_add, field_type, precision, scale, length)

    def create_headers(self):
        cred_values = self.get_cred_values(self.config.prod_cred_file)
        headers = {'Authorization': self.basic_auth(cred_values[0], cred_values[1])}
        return headers

    def request_json_as_text(self, wfs_url, headers, layer_name):
        params = dict(service='WFS', version="1.0.0", request='GetFeature', typeName=layer_name, outputFormat='json')
        r = requests.get(wfs_url, params=params, headers=headers)
        text = r.text
        return text

    def write_text_as_json_file(self, layer_name, text):
        out_file = os.path.join(self.config.json_conversion_temp, layer_name + ".json")
        f = open(out_file, 'w')
        f.write(text)
        f.close()
        return out_file

    def get_layer_names_from_lyrx_files(self, lyrx_list):
        layer_names = []
        for lyrx in lyrx_list:
            layer_names.append(os.path.basename(lyrx))
        return layer_names
