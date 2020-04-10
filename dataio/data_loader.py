import arcpy
import os
import sys
from datetime import datetime
import openpyxl

class DataLoad():
    def __init__(self):

        #self.config = config
        self.sde_connections = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\03_RRAD\CCSP_Data_Management_ToolBox\connection_files"
        self.ETL_load_base_folder = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB"
        self.ETL_source_table = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\ETL_input_data_sources.xlsx"

    #TODO - need tests for all of these methods
    #TODO - should prob relocate a lot of these methods
    #note - if xlxs was csv instead then we could version contol it

    def todays_gdb_name(self):
        basename = "PipXP_"
        today = datetime.today().strftime('%Y%m%d')
        extension = ".gdb"
        full_name = basename + today + extension
        return full_name

    def todays_gdb_full_path_name(self):
        full_name = self.todays_gdb_name()
        full_path = os.path.join(self.ETL_load_base_folder, full_name)
        return full_path

    def create_todays_gdb(self):
        if arcpy.Exists(self.todays_gdb_full_path_name()) == True:
            arcpy.AddError("gdb already exists")
            arcpy.ExecuteError()
            os._exit()
        else:
            print "Creating gdb"
            arcpy.CreateFileGDB_management(self.ETL_load_base_folder, self.todays_gdb_name())

    def read_xlsx_as_sheet_object(self):
        #print "Reading xlsx"
        wb_obj = openpyxl.load_workbook(self.ETL_source_table)
        sheet = wb_obj.active
        return sheet

    def read_two_sheet_columns_as_dict(self):
        print "Creating dictionary from xlsx"
        data_dict = {}
        for row in self.read_xlsx_as_sheet_object().iter_rows(min_row=2, values_only=True):
            if row[0] is not None and "#" not in row[0]:
                data_dict[row[0]] = row[1]
        return data_dict

    def source_formatter(self, source_string):
        if r"\\" in source_string:
            return source_string
        else:
            return os.path.join(self.sde_connections, source_string)

    def valid_source_values(self, data_dict):
        valid = True
        for key, value in data_dict.iteritems():
            full_source = self.source_formatter(value)
            if arcpy.Exists(full_source) == False:
                print "Invalid source for: " + str(key)
                valid = False
        return valid

    # make query layer for views? not sure if required if just doing a copy
    def copy_sources(self, data_dict):
        if self.valid_source_values(data_dict) == True:
            print "Coping data sources to the gdb"
            for key, value in data_dict.iteritems():
                print "   Copying: " + str(key)
                full_input_path = self.source_formatter(value)
                full_output_path = os.path.join(self.todays_gdb_full_path_name(), key)
                try:
                    arcpy.Copy_management(full_input_path, full_output_path)
                except Exception:
                    arcpy.FeatureClassToFeatureClass_conversion(full_input_path, self.todays_gdb_full_path_name(), key)
                except:
                    arcpy.TableToTable_conversion(full_input_path, self.todays_gdb_full_path_name(), key)
        else:
            arcpy.AddError("Invalid data source(s)")
            arcpy.ExecuteError()
            raise Exception

    def load_data_to_gdb(self):
        try:
            self.create_todays_gdb()
            self.copy_sources(self.read_two_sheet_columns_as_dict())
        except:
            if arcpy.Exists(self.todays_gdb_full_path_name()) == True:
                arcpy.Delete_management(self.todays_gdb_full_path_name)
            arcpy.AddError("Data could not be loaded")
            arcpy.ExecuteError()

