import arcpy
import os
from datetime import datetime
import openpyxl


class DataLoad():
    def __init__(self):

        #output
        self.base_folder = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB"

        #inputs
        sde_connections = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\03_RRAD\CCSP_Data_Management_ToolBox\connection_files"

        EMGAATS_sde = "BESDBPROD1.EMGAATS.sde"
        PLT_sde = "BESDBPROD1.PLT.sde"
        REHAB_sde = "BESDBPROD1.REHAB.sde"
        HANSEN8_SG_sde = "BESDBTEST1.HANSEN8_SG"
        MODEL_CATALOG_sde = "BESDBTEST1.MODELCATALOG.sde"
        CCSP_sde = "BESDBTEST1.CCSP.sde"
        BESGEORPT_sde = "BESDBTEST1.BESGEORPT.sde"
        EGH_PUBLIC_sde = "GISDB1.EGH_PUBLIC.sde"
        PWB_WATER_sde = "GISDB1.PWBWATER.sde"

        self.EMGAATS_sde_path = os.path.join(sde_connections, EMGAATS_sde)
        self.PLT_sde_path = os.path.join(sde_connections, PLT_sde)
        self.REHAB_sde_path = os.path.join(sde_connections, REHAB_sde)
        self.HANSEN8_SG_sde_path = os.path.join(sde_connections, HANSEN8_SG_sde)
        self.MODEL_CATALOG_sde_path = os.path.join(sde_connections, MODEL_CATALOG_sde)
        self.CCSP_sde_path = os.path.join(sde_connections, CCSP_sde)
        self.EGH_PUBLIC_sde_path = os.path.join(sde_connections, EGH_PUBLIC_sde)
        self.BESGEORPT_sde_path = os.path.join(sde_connections, BESGEORPT_sde)
        self.PWB_WATER_sde_path = os.path.join(sde_connections, PWB_WATER_sde)

        # idea - move all input to xls. read in as dict then feed dict into copy method

        self.costest_hardareas = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Refs\PipXP\PipXP_GOLEM.gdb\COSTEST_HARDAREAS"
        self.pressure_Mains = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\Refs\PipXP\PipXP_20191113.gdb\pressureMain_erase"
        # self.pressure_Mains = self.PWB_WATER_sde_path + r"\GISDB1.PWBWATER.WaterGIS.PressurizedMain" - do Status <> 'REM' in appsettings
        # modify DME_master_hybrid script to spit out both all city and ccsp results simultaneously?
        # self.DME_master_hybrid_ccsp = self.CCSP_sde_path + r"\CCSP.GIS.DME_master_hybrid"

        self.DME_master_hybrid = self.CCSP_sde_path + r"\CCSP.GIS.DME_master_hybrid"

        self.ms4_outfall_boundaries = self.EGH_PUBLIC_sde_path + r"\EGH_PUBLIC.ARCMAP_ADMIN.of_drainage_bounds_bes_pdx"
        self.lust_bes_pdx = self.EGH_PUBLIC_sde_path + r"\EGH_Public.ARCMAP_ADMIN.lust_bes_pdx"
        self.ecsi_sites = self.EGH_PUBLIC_sde_path + r"\EGH_Public.ARCMAP_ADMIN.ECSI_SITES_BES_PDX"
        self.zoning = self.EGH_PUBLIC_sde_path + r"\EGH_PUBLIC.ARCMAP_ADMIN.zoning_pdx"
        self.major_gas_lines = self.EGH_PUBLIC_sde_path + r"\EGH_PUBLIC.ARCMAP_ADMIN.major_gas_lines_metro"
        self.fiber_routes = self.EGH_PUBLIC_sde_path + r"\EGH_Public.ARCMAP_ADMIN.FIBER_ROUTES_PDX"
        self.light_rail_lines = self.EGH_PUBLIC_sde_path + r"\EGH_Public.ARCMAP_ADMIN.LIGHT_RAIL_LINES_METRO"
        self.railroads = self.EGH_PUBLIC_sde_path + r"\EGH_Public.ARCMAP_ADMIN.RAILROADS_METRO"
        self.streets = self.EGH_PUBLIC_sde_path + r"\EGH_Public.ARCMAP_ADMIN.STREETS_PDX"
        self.ten_percent_slope = self.EGH_PUBLIC_sde_path + r"\EGH_PUBLIC.ARCMAP_ADMIN.slope_10_percent_lidar_pdx"
        self.vulnerability_risk_factors = self.EGH_PUBLIC_sde_path + r"\EGH_Public.ARCMAP_ADMIN.vulnerability_risk_factors_pdx"
        self.bes_collection_nodes = self.EGH_PUBLIC_sde_path + r"\EGH_PUBLIC.ARCMAP_ADMIN.collection_points_bes_pdx"

        self.emgaats_areas = self.EMGAATS_sde_path + r"\EMGAATS.GIS.Areas"

        # these may need to actually come from HANSEN8_SG - check
        self.peakScore_Lookup = self.REHAB_sde_path + r"\REHAB.dbo.REHAB_PeakScore_Lookup"
        self.tblPipeTypeRosetta = self.REHAB_sde_path + r"\REHAB.dbo.REHAB_tblPipeTypeRosetta"
        self.tblRemainingUsefulLifeLookup = self.REHAB_sde_path + r"\REHAB.tblRemainingUsefulLifeLookup"
        self.branches = self.REHAB_sde_path + r"\REHAB.GIS.REHAB_Branches"
        # self.street_type_lookup = verify if we include or put in appsettings - manually added so I don't know where this is

        self.Simulation = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.Simulation"
        self.LinkResults = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.LinkResults"
        self.NodeFloodingResults = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.NodeFloodingResults"
        self.NodeResults = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.NodeResults"
        self.AreaResults = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.AreaResults"
        self.Areas = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.Areas"
        self.Links = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.Links"
        self.Nodes = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.Nodes"
        self.ModelTracking = self.MODEL_CATALOG_sde_path + r"\MODEL_CATALOG.GIS.ModelTracking"
        self.MAU = self.BESGEORPT_sde_path + r"\BESGEORPT.GIS.MAU_bes_pdx"

        # HANSEN8 SG sources - possibly temporary
        STMNINDHIST = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_STORM.STMNINDHIST"
        STMNSERVINSPOB = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_STORM.STMNINSPOBPOS"
        STMNSERVICEINSP = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_STORM.STMNSERVICEINSP"
        COMPSTMN = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_STORM.COMPSTMN"
        SMNINDHIST = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_SEWER.SMNINDHIST"
        SMNSERVINSPOB = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_SEWER.SMNSERVINSPOB"
        SMNSERVICEINSP = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_SEWER.SMNSERVICEINSP"
        COMPSMN = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_SEWER.COMPSMN"
        ASSETINSPINDEX = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT.ASSETINSPINDEX"
        COMPTYPE = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT.COMPTYPE"
        STMNSERVINSPTYPEOB = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_STORM.STMNSERVINSPTYPEOB"
        SMNSERVINSPTYPEOB = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_SEWER.SMNSERVINSPTYPEOB"
        STMNSERVINSPTYPEOBSEV = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_STORM.STMNSERVINSPTYPEOBSEV"
        SMNSERVINSPTYPEOBSEV = HANSEN8_SG_sde + r"\HANSEN8_SG.ASSETMANAGEMENT_SEWER.SMNSERVINSPTYPEOBSEV"

        # Views
        self.bes_collection_lines = self.BESGEORPT_sde_path + r"\GEORPT.v_collection_lines_ACTIVE_BES_SEWER_STORM"
        self.PLT_PROJECT_PIPES_IN_DESIGN = self.PLT_sde_path + r"\PLT.dbo.V_PROJECT_PIPES_IN_DESIGN"
        self.LDSRP_STATUS_BPW = self.REHAB_sde_path + r"\REHAB.dbo.V_LDSRP_STATUS_BPW"

    def create_gdb(self):
        basename = "PipXP_"
        today = datetime.today().strftime('%Y%m%d')
        extension = ".gdb"
        full_name = basename + today + extension
        if arcpy.Exists(full_name):
            print "gdb already exists"
        else:
            arcpy.CreateFileGDB(self.base_folder, full_name)

    # make query layer for views? not sure if required if just doing a copy

    def read_xlsx_as_dict(self, xlsx_source):
        data_dict = {}
        wb_obj = openpyxl.load_workbook(xlsx_source)
        sheet = wb_obj.active
        for row in sheet.iter_rows(min_row = 2, values_only = True):
            if row[0] is not None and "#" not in row[0]:
                data_dict[row[0]] = row[1] # assumes xlsx only has 2 columns - can we get count of columns from object?
        return data_dict

    def valid_source(self, data_dict):
        for value in data_dict.values():
            if "\\" in value:
                arcpy.Exists(value)
            else:
                arcpy.Exists(os.path.join(self.sde_connections, item))

    def Copy(self):
        for value in data_dict.values():

        #arcpy.FeatureClassToFeatureClass_conversion