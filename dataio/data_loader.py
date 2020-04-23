import arcpy
import os
import sys
import openpyxl
from dataio import utility
from businessclasses import config
from datetime import datetime

test_flag = "TEST"

class DataLoad:
    def __init__(self):

        self.config = config.Config(test_flag)
        self.utility = utility.Utility(self.config)
        self.todays_gdb_full_path_name = self.utility.todays_gdb_name(self.utility, datetime.today())

    def create_todays_gdb(self):
        if arcpy.Exists(self.todays_gdb_full_path_name):
            #arcpy.AddError("gdb already exists")
            arcpy.ExecuteError()
            sys.exit("gdb already exists") #TODO - make sure this works as expected
        else:
            print "Creating gdb"
            arcpy.CreateFileGDB_management(self.config.ETL_load_base_folder, self.utility.todays_gdb_name(self.utility))

    # note - if xlxs was csv instead then we could version control it
    def read_xlsx_as_sheet_object(self):
        #print "Reading xlsx"
        wb_obj = openpyxl.load_workbook(self.config.ETL_source_table)
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
        if self.utility.valid_source_values(data_dict):
            print "Coping data sources to the gdb"
            for key, value in data_dict.iteritems():
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

    def load_data_to_gdb(self):
        try:
            self.create_todays_gdb()
            self.copy_sources(self.read_two_sheet_columns_as_dict())
        except:
            if arcpy.Exists(self.todays_gdb_full_path_name):
                arcpy.Delete_management(self.todays_gdb_full_path_name)
            arcpy.AddError("Data could not be loaded")
            arcpy.ExecuteError()

