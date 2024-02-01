# CCSP_arcpy_Tools

Tools related to CCSP

1. dme_master_hybrid - Prepare/condition master data and DME data for input into the CCSP ETL.

2. data_loader - The entries in the appsettings.json (used by CCSP ETL) must exist in the input sources json file (name: file location) and if not the process will fail.  Any 'extras' that are in the input sources file but not in the appsettings file will be still copied. A successful load will copy all data from their source into a date stamped gdb at \\besfile1\ccsp\03_WP2_Planning_Support_Tools\04_CostEstimator\Code\InputGDB, named CCSPToolsInput_DATE.gdb.
