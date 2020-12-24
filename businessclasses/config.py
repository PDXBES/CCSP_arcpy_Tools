import os
import arcpy

try:
    from typing import Dict
    from typing import List
except:
    pass

class Config:
    def __init__(self, test_flag):
        init_options = {"PROD": 0, "TEST": 1}

        self.test_flag = test_flag

        self.sde_connections = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\03_RRAD\CCSP_Data_Management_ToolBox\connection_files"
        self.ETL_load_base_folder = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\CCSP_Tools_Input"
        # for besapp4 testing
        #self.ETL_load_base_folder = r"C:\temp\CCSP_tools_working"

        self.log_file = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\CCSP_Tools_Input\data_load_log"
        #self.log_file = r"\\besapp4\gis\Scripts\Python\Production\CCSP\data_load_log"

        ##
        server = None

        if init_options[test_flag] == 1:
            server = "BESDBTEST1"
        elif init_options[test_flag] == 0:
            server = "BESDBPROD1"

        GISDB1 = "GISDB1.EGH_PUBLIC.sde"
        EMGAATS_sde = server + ".EMGAATS.sde"
        CCSP_sde = server + ".CCSP.sde"
        BESGEORPT_sde = server + ".BESGEORPT.sde"
        self.GIS_TRANSFER10_sde = server + ".GIS_TRANSFER10.sde"

        self.egh_public = os.path.join(self.sde_connections, GISDB1)
        self.DME_table_name = r"EGH_Public.ARCMAP_ADMIN.collection_lines_bes_pdx"
        self.DME_sde_path = self.egh_public + r"\\" + self.DME_table_name

        self.GIS_TRANSFER10_sde_path = os.path.join(self.sde_connections, self.GIS_TRANSFER10_sde)
        self.GIS_TRANSFER10_table_name = r"GIS_TRANSFER10.GIS.collection_lines_bes_pdx"
        self.TRANSFER10_collection_lines_path = self.GIS_TRANSFER10_sde_path + r"\\" + self.GIS_TRANSFER10_table_name

        self.EMGAATS_sde_path = os.path.join(self.sde_connections, EMGAATS_sde)
        self.master_links_sde_path = self.EMGAATS_sde_path + r"\EMGAATS.GIS.Links"
        self.master_nodes_sde_path = self.EMGAATS_sde_path + r"\EMGAATS.GIS.Nodes"

        self.CCSP_sde_path = os.path.join(self.sde_connections, CCSP_sde)
        self.DME_view_name = r"CCSP.GIS.VDME"
        self.Master_links_nodes_view_name = r"CCSP.GIS.V_master_link_with_nodes"
        self.CCSP_Master_links_nodes_view_name = r"CCSP.GIS.v_ccsp_master_link_with_nodes"
        # TODO - consider pointing directly to Sitka InputGDB - standardize gdb name (currently date stamped therefore dynamic)
        self.DME_master_hybrid_id_table_sde_path = self.CCSP_sde_path + r"\CCSP.GIS.Current_ID"
        self.DME_master_hybrid_sde_path = self.CCSP_sde_path + r"\CCSP.GIS.DME_master_hybrid"

        # self.DME_master_hybrid_gdb_path = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\CCSP_Tools_Input\CCSPToolsInput.gdb"

        ## placeholders for use once we switch to using view/QL instead of hard coded query in dme_master_hybrid.create_dme_links()
        self.BESGEORPT_sde_path = os.path.join(self.sde_connections, BESGEORPT_sde)
        self.collection_lines_ACTIVE_BES_SEWER_STORM_view_name = r"\BESGEORPT.GIS.v_collection_lines_ACTIVE_BES_SEWER_STORM"
        self.collection_lines_ACTIVE_BES_SEWER_STORM_view_name_path = self.BESGEORPT_sde_path + self.collection_lines_ACTIVE_BES_SEWER_STORM_view_name




##


##








