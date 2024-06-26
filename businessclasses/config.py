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

        self.loader_output_base_folder = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\CCSPTools\DataLoader\DataLoaderOutput\Production"
        #self.loader_output_base_folder = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\CCSPTools\DataLoader\DataLoaderOutput\Test" #for manual test run

        self.loader_input_base_folder = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\CCSPTools\DataLoader\DataLoaderInputFiles"

        self.WFS_layers = r"\\besfile1\ccsp\Mapping\Lyr\lyrx\ESA_WFS_layers"
        #self.WFS_layers = r"\\besfile1\ccsp\Mapping\Lyr\lyrx\ESA_WFS_layers_QA"
        #self.WFS_layers = r"\\besfile1\ccsp\Mapping\Lyr\lyrx\ESA_WFS_layers_testing"
        self.WFS_intermediate_gdb = r"\\besfile1\ccsp\Mapping\ArcPro_Projects\WFS_setup\WFS_intermediate.gdb"
        self.json_conversion_temp = r"\\besfile1\ccsp\Mapping\JSON\conversion_temp"
        self.intermediate_gdb = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\CCSPTools\DataLoader\DataLoaderOutput\Production\Intermediate\intermediate.gdb"
        self.archive_folder = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\CCSPTools\DataLoader\DataLoaderOutput\Production\Archive"
        self.log_file = r"\\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\CCSPTools\DataLoader\DataLoaderOutput\Production\data_load_log"

        # PROD
        self.wfs_url = 'https://geoserver-ccsptools.gov-prod.sitkatech.com/geoserver/CCSP/ows'
        # QA
        # self.wfs_url = 'https://geoserver-ccsptools.gov-qa.sitkatech.com/geoserver/CCSP/ows'

        self.prod_cred_file = r"\\besfile1\ccsp\Mapping\dev\cred_file.txt"

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
        self.GIS_TRANSFER10_GIS_sde = server + ".GIS_TRANSFER10.GIS.sde"

        self.egh_public = os.path.join(self.sde_connections, GISDB1)
        self.DME_table_name = r"EGH_Public.ARCMAP_ADMIN.collection_lines_bes_pdx"
        self.DME_sde_path = self.egh_public + r"\\" + self.DME_table_name

        self.GIS_TRANSFER10_sde_path = os.path.join(self.sde_connections, self.GIS_TRANSFER10_sde)
        self.GIS_TRANSFER10_GIS_sde_path = os.path.join(self.sde_connections, self.GIS_TRANSFER10_GIS_sde)
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

        self.block_object_exclusions_raw_source = r"\\besfile1\ccsp\Mapping\Gdb\heat_mapping.gdb\block_object_exclusion"
        self.block_object_exclusions_fl = arcpy.MakeFeatureLayer_management(self.block_object_exclusions_raw_source,
                                                                       r"in_memory/block_object_exclusions_fl",
                                                                       "Excluded = 1")
        self.block_object_exclusions_copy = arcpy.CopyFeatures_management(self.block_object_exclusions_fl,
                                                          r"in_memory/block_object_exclusions_copy")

        self.MAU_exlusions_raw_source = r"\\besfile1\ccsp\Mapping\Gdb\heat_mapping.gdb\MAU_exclusion"
        self.MAU_exclusions_fl = arcpy.MakeFeatureLayer_management(self.MAU_exlusions_raw_source,
                                                              r"in_memory/MAU_exclusions_fl",
                                                              "Excluded = 1")
        self.MAU_exclusions_copy = arcpy.CopyFeatures_management(self.MAU_exclusions_fl,
                                                            r"in_memory/MAU_exclusions_copy")

        self.sewer_basin_exclusions_raw_source = r"\\besfile1\ccsp\Mapping\Gdb\heat_mapping.gdb\basin_exclusion"
        self.sewer_basin_exclusions_fl = arcpy.MakeFeatureLayer_management(self.sewer_basin_exclusions_raw_source,
                                                              r"in_memory/sewer_basin_exclusions_fl",
                                                              "Excluded = 1")
        self.sewer_basin_exclusions_copy = arcpy.CopyFeatures_management(self.sewer_basin_exclusions_fl,
                                                                    r"in_memory/sewer_basin_exclusions_copy")


        # self.DME_master_hybrid_gdb_path = r"\\besfile1\CCSP\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB\CCSP_Tools_Input\CCSPToolsInput.gdb"

        ## placeholders for use once we switch to using view/QL instead of hard coded query in dme_master_hybrid.create_dme_links()
        self.BESGEORPT_sde_path = os.path.join(self.sde_connections, BESGEORPT_sde)
        self.collection_lines_ACTIVE_BES_SEWER_STORM_view_name = r"\BESGEORPT.GIS.v_collection_lines_ACTIVE_BES_SEWER_STORM"
        self.collection_lines_ACTIVE_BES_SEWER_STORM_view_name_path = self.BESGEORPT_sde_path + self.collection_lines_ACTIVE_BES_SEWER_STORM_view_name

        self.rename_dict = {
            'DepthToHGLFtOnlyForBasementAboveCrownOfPipe': 'DepthToHGLFtOFBACoP',
            'EstimatedBasementFloodingDepthFtOnlyForBasementAboveCrownOfPipe': 'EstBsmtFloodingDepthFtOFBACoP',
            'ModelLinkIsUpstreamNodeSurcharged': 'ML_IsUSNodeSurcharged',
            'ModelLinkIsHydraulicallyDeficient': 'ML_IsHydraulDef',
            'ModelLinkExceedsSurchargedDepthCriteria': 'ML_ExceedsSurchargedDepthCrit',
            'ModelLinkExceedsSurchargeDurationCriteria': 'ML_ExceedsSurchargeDurCrit',
            'SmallDiameterRehabStatusInProject': 'SmallDiamRehabStatusInProject',
            'LargeDiameterRehabStatusInProject': 'LargeDiamRehabStatusInProject',
            'MortalityCOFMaxSegmentWithSpotRepairPipeEmergencyRepair': 'MortCOFMaxSegWSpotPipeERep',
            'MortalityCOFMaxSegmentWithCippEmergencyRepair': 'MortCOFMaxSegWCippERep',
            'MortalityCOFMaxSegmentWithCippNoSpotRepairPipeEmergencyRepair': 'MrtCOFMxSegWCippNSptRepPipERep',
            'MortalityCOFMaxSegmentWithWholePipeReplacementEmergencyRepair': 'MortCOFMaxSegWWholePipReplERep',
            'NBCRLifeCycleWholePipeReplacement': 'NBCRLifeCycleWholePipRepl',
            'WholePipeReplacementCapitalCostNBCR': 'WholePipeReplCapitalCostNBCR',
            'WholePipeReplacementCapitalCost': 'WholePipeReplCapitalCost',
            'NumberOfSpotRepairOnlyIfNotLined': 'NumOfSpotRepOnlyIfNotLined',
            'RehabReportMortalityCOFLinkSegmentID': 'RehabMortCOFLinkSegmentID',
            'SpotRepairConstructionDurationBase': 'SpotRepairConstrDurBase',
            'IncludeWaterlineRelocationCosts': 'IncludeWaterlineRelocCosts',
            'COFBasementFloodingPublicInconvenience': 'COFBsmntFloodPubInconv',
            'COFBasementFloodingPublicSafety': 'COFBsmntFloodPubSafety',
            'COFStabilizationSignageAndTrafficControl': 'COFSSAndTrafficControl',
            'MortalityCOFForceMainWithSpotEmergencyRepair': 'MortCOFForceMainWSpotERepair',
            'MortalityCOFForceMainNoEmergencyRepair': 'MortCOFForceMainNoERepair',
            'MortalityCOFWithSpotEmergencyRepair': 'MortCOFWSpotERepair',
            'SewerDesignManualPeakSanitaryFlowCFS': 'SewerDesignManPeakSanFlowCFS',
            'IsPositiveDryWeatherFlowGreaterThanDesignFlow': 'IsPosDryFlwGrtThnDesignFlw',
            'IsPositiveAvgWinterFlowGreaterThanDesignFlow': 'IsPosAvgWintFlwGrtThnDesignFlw',
            'IsAvgWinterFlowLessThanDryWeatherFlow': 'IsAvgWintFlwLessThnDryFlow',
            'BlockObjectMortalityRiskPerAcre': 'BlockObjMortRiskPerAcre',
            'PVFactorWholePipeAfterPlannedSpotAndEmerCIPP': 'PVFactWhoPipAftPlanSpotnECIPP',
            'PVFactorWholePipeAfterPlannedSpotAndEmerCIPPAndWholePipe': 'PVFactWPAftPlanSpotnECIPPnWP',
            'CAMGWholePipeReplacementCapitalCost': 'CAMGWhoPipReplCapitalCost'
        }

        # only used for "alternate" method (csv to json to fc)
        self.layer_names = [
            "CCSPCharacterizationAreas",
            "CCSPCharacterizationDetailedBypassPumpingLinks",
            "CCSPCharacterizationLinks",
            "CCSPCharacterizationNodes",
            "CCSPCharacterizationStormwaterLinks",

            "CharacterizationAreas",
            "CharacterizationDetailedBypassPumpingLinks",
            "CharacterizationLinks",
            "CharacterizationNodes",
            "CharacterizationStormwaterLinks",

            "DashboardForecastingReportAreas",
            "DashboardForecastingReportBlockObjects",
            "DashboardForecastingReportLinks",
            "DashboardForecastingReportMAUs",
            "DashboardForecastingReportNodes",
            "DashboardForecastingReportProjects",
            "DashboardForecastingReportSewerBasins",

            "RehabLinks",
            "RehabReportLinkFMEs",
            "RehabReportLinks",
            "RehabReportMortalityCOFLinkSegments",
            "RehabReportPipXPLinkSegments",
            "RehabReportRULLinkSegments"
        ]

        # areas that we want to subset to only those that have registered Characterizations in Model Catalog
        # dict format is: CCSP fc name: [CCSP fc key field, exclusion source, exclusion key field]
        # kinds fragile - relies on exclusion sources not changing location or field properties
        self.CCSP_subset_layers = {
            "ESA_DashboardForecastingReportBlockObjects": ["BlockObjectID",
                                                           self.block_object_exclusions_copy,
                                                           "block_object_ID"],
            "ESA_DashboardForecastingReportMAUs": ["MaintenanceAreaUnitID",
                                                   self.MAU_exclusions_copy,
                                                   "MAUID"],
            "ESA_DashboardForecastingReportSewerBasins": ["SewerBasinID",
                                                          self.sewer_basin_exclusions_copy,
                                                          "BASIN_ID"]
        }












