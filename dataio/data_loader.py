import arcpy
import os
from datetime import datetime
import openpyxl


class DataLoad():
    def __init__(self):

        self.todays_gdb = ""

        self.base_folder = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB"

        self.sde_connections = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\03_RRAD\CCSP_Data_Management_ToolBox\connection_files"

    #TODO - need test for all of these methods
    #TODO - figure out if some of these methods should relocated
    #TODO - need to create gdb first but want to delete gdb if any sources are invalid or if fc to fc fails

    def generate_todays_gdb(self):
        basename = "PipXP_"
        today = datetime.today().strftime('%Y%m%d')
        extension = ".gdb"
        full_name = basename + today + extension
        self.todays_gdb = full_name

    def create_gdb(self):
        if arcpy.Exists(self.todays_gdb):
            print "gdb already exists"
        else:
            arcpy.CreateFileGDB_management(self.base_folder, self.todays_gdb)

    def read_xlsx_as_dict(self, xlsx_source):
        data_dict = {}
        wb_obj = openpyxl.load_workbook(xlsx_source)
        sheet = wb_obj.active
        for row in sheet.iter_rows(min_row = 2, values_only = True):
            if row[0] is not None and "#" not in row[0]:
                data_dict[row[0]] = row[1] # assumes xlsx only has 2 columns - can we get count of columns from object?
        return data_dict

    def source_formatter(self, source_string):
        if r"\\" in source_string:
            return source_string
        else:
            return os.path.join(self.sde_connections, source_string)

    def valid_source(self, data_dict):
        for value in data_dict.values():
            if arcpy.Exists(self.source_formatter(value)) == False:
                return False

    # make query layer for views? not sure if required if just doing a copy

    def Copy(self, data_dict):
        if self.valid_source(data_dict) == True:
        for key, value in data_dict.iteritems():
            arcpy.FeatureClassToFeatureClass_conversion(value, self.todays_gdb, key)

