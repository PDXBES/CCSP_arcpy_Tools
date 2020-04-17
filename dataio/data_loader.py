import arcpy
import os
import sys
import openpyxl
from dataio.utility import todays_gdb_name, todays_gdb_full_path_name, source_formatter, valid_source_values


class DataLoad():
    def __init__(self):

        #TODO - move these to config and figure out how to import properly
        self.sde_connections = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\03_RRAD\CCSP_Data_Management_ToolBox\connection_files"
        self.ETL_load_base_folder = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB"
        self.ETL_source_table = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\ETL_input_data_sources.xlsx"

    #TODO - need tests for all of these methods
    #note - if xlxs was csv instead then we could version contol it

    def create_todays_gdb(self):
        if arcpy.Exists(todays_gdb_full_path_name(self.ETL_load_base_folder)) == True:
            #arcpy.AddError("gdb already exists")
            arcpy.ExecuteError()
            sys.exit("gdb already exists") #TODO - make sure this works as expected
        else:
            print "Creating gdb"
            arcpy.CreateFileGDB_management(self.ETL_load_base_folder, todays_gdb_name())

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

    def copy_sources(self, data_dict):
        if valid_source_values(self.sde_connections, data_dict) == True:
            print "Coping data sources to the gdb"
            for key, value in data_dict.iteritems():
                print "   Copying: " + str(key)
                full_input_path = source_formatter(self.sde_connections, value)
                full_output_path = os.path.join(todays_gdb_full_path_name(self.ETL_load_base_folder), key)
                try:
                    arcpy.Copy_management(full_input_path, full_output_path)
                except Exception:
                    arcpy.FeatureClassToFeatureClass_conversion(full_input_path, todays_gdb_full_path_name(
                        self.ETL_load_base_folder), key)
                except:
                    arcpy.TableToTable_conversion(full_input_path, todays_gdb_full_path_name(self.ETL_load_base_folder), key)
        else:
            arcpy.AddError("Invalid data source(s)")
            arcpy.ExecuteError()
            raise Exception

    def load_data_to_gdb(self):
        try:
            self.create_todays_gdb()
            self.copy_sources(self.read_two_sheet_columns_as_dict())
        except:
            if arcpy.Exists(todays_gdb_full_path_name(self.ETL_load_base_folder)) == True:
                arcpy.Delete_management(todays_gdb_full_path_name)
            arcpy.AddError("Data could not be loaded")
            arcpy.ExecuteError()

