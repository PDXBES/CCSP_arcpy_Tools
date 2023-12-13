import os
import arcpy
from dataio.data_loader import DataLoad

data_load = DataLoad()
utility = data_load.utility

arcpy.env.overwriteOutput = True

json_folder = r"C:\temp\WFS"
#json_file = "RehabReportMortalityCOFLinkSegments_sub_TEST.json"
#json_file = "RehabReportRULLinkSegments.json"
json_file = "RehabReportMortalityCOFLinkSegments_prod.json"

json_file = os.path.join(json_folder, json_file)

output_gdb = r"C:\temp_work\working.gdb"

print("reading json into dict")
json_as_dict = utility.create_dict_from_json(json_file)
print("   there are {} features in the dict".format(utility.get_count_of_features_in_json_dict(json_as_dict)))

print("splitting json dict into 2 dicts - list output")
split_list = utility.split_json_as_dict(json_as_dict)

for json_dict in split_list:
    index_num = str(split_list.index(json_dict))
    json_name = "dict_to_json" + index_num + ".json"
    json_outfile = os.path.join(json_folder, json_name)
    print("writing {} to {} as json file".format(json_name, json_folder))
    print("   there are {} features in this dict".format(utility.get_count_of_features_in_json_dict(json_dict)))
    if os.path.exists(json_outfile):
        os.remove(json_outfile)
    utility.write_json_to_disk(json_dict, json_outfile)

    fc_name = "json_to_fc" + index_num
    fc_outfile = os.path.join(output_gdb, fc_name)
    print("writing {} to {} as fc".format(fc_name, output_gdb))
    arcpy.conversion.JSONToFeatures(json_outfile, fc_outfile)



