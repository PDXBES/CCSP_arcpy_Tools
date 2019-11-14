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


        sde_connections = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\03_RRAD\CCSP_Data_Management_ToolBox\connection_files"
##
        server = None

        if init_options[test_flag] == 1 :
            server = "BESDBTEST1"
        elif init_options[test_flag] == 0:
            server = "BESDBPROD1"


        EMGAATS_sde = server + ".EMGAATS.sde"
        CCSP_sde = server + ".CCSP.sde"
        GISDB1 = "GISDB1.EGH_PUBLIC.sde"

        self.egh_public = os.path.join(sde_connections, GISDB1)
        self.DME_table_name = r"EGH_Public.ARCMAP_ADMIN.collection_lines_bes_pdx"
        self.DME_sde_path = self.egh_public + r"\\" + self.DME_table_name

        self.EMGAATS_sde_path = os.path.join(sde_connections, EMGAATS_sde)

        self.master_links_sde_path = self.EMGAATS_sde_path + r"\EMGAATS.GIS.Links"
        self.master_nodes_sde_path = self.EMGAATS_sde_path + r"\EMGAATS.GIS.Nodes"

        self.CCSP_sde_path = os.path.join(sde_connections, CCSP_sde)
        self.DME_view_name = r"CCSP.GIS.VDME"
        self.Master_links_nodes_view_name = r"CCSP.GIS.V_master_link_with_nodes"
        self.CCSP_Master_links_nodes_view_name = r"CCSP.GIS.v_ccsp_master_link_with_nodes"
        self.DME_master_hybrid_id_table_sde_path = self.CCSP_sde_path + r"\CCSP.GIS.Current_ID"
        self.DME_master_hybrid_sde_path = self.CCSP_sde_path + r"\CCSP.GIS.DME_MASTER_HYBRID"


##


##








