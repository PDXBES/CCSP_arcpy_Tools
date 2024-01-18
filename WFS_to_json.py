from dataio import utility
from businessclasses import config
import geopandas as gpd
import pandas as pd
import requests
import sys
import urllib
import io
from shapely import wkt
from arcgis.features import GeoAccessor
import requests as r
import os
import arcpy


arcpy.env.overwriteOutput = True

test_flag = "PROD"
config = config.Config(test_flag)
utility = utility.Utility(config)

# PROD
wfs_url = 'https://geoserver-ccsptools.gov-prod.sitkatech.com/geoserver/CCSP/ows'
# QA
#wfs_url = 'https://geoserver-ccsptools.gov-qa.sitkatech.com/geoserver/CCSP/ows'

# RehabReportMortalityCOFLinkSegments is the only one giving us an issue
layers = [
            # "CCSPCharacterizationAreas",
            # "CCSPCharacterizationDetailedBypassPumpingLinks",
            # "CCSPCharacterizationLinks",
            # "CCSPCharacterizationNodes",
            # "CCSPCharacterizationStormwaterLinks ",
            #
            # "CharacterizationAreas",
            # "CharacterizationDetailedBypassPumpingLinks",
            # "CharacterizationLinks",
            # "CharacterizationNodes",
            # "CharacterizationStormwaterLinks",
            #
            # "RehabReportLinkFMEs",
            # "RehabReportLinks",
            # "RehabReportRULLinkSegments",
            "RehabReportMortalityCOFLinkSegments",
            # "RehabReportPipXPLinkSegments",
            #
            # "DashboardForecastingReportAreas",
            # "DashboardForecastingReportBlockObjects",
            # "DashboardForecastingReportLinks",
            # "DashboardForecastingReportMAUs",
            # "DashboardForecastingReportNodes",
            # "DashboardForecastingReportProjects",
            # "DashboardForecastingReportSewerBasins"
         ]

cred_values = utility.get_cred_values(config.prod_cred_file)
headers = {'Authorization': utility.basic_auth(cred_values[0], cred_values[1])}

print(r.certs.where())
for layer in layers:
    try:
        utility.datetime_print("pulling WFS data for {}".format(layer))
        params = dict(service='WFS', version="1.0.0", request='GetFeature', typeName=layer, outputFormat='json')
        # r = requests.get(wfs_url, params=params, headers=headers, verify=False)
        r = requests.get(wfs_url, params=params, headers=headers)

        utility.datetime_print("writing to json")
        text = r.text
        out_file = os.path.join(config.json_conversion_temp, layer + ".json")
        f = open(out_file, 'w')
        f.write(text)
        f.close()


        utility.datetime_print("converting json to feature class")
        # seeems like feature from this method must be written out to disk, cannot use in_memory version to proceed
        #in_memory_feature = arcpy.conversion.JSONToFeatures(out_file, 'in_memory/' + layer)
        feature = arcpy.conversion.JSONToFeatures(out_file,
                                                  os.path.join(config.WFS_intermediate_gdb, layer))

        utility.datetime_print("shortening fields")
        #fl = utility.make_in_memory_feature_layer(feature, layer)
        working_memory = utility.shorten_field_names(layer, feature)

        utility.datetime_print("adding geom_length")
        utility.add_and_populate_length_field(working_memory, 'geom_length')

        output_fc = os.path.join(config.GIS_TRANSFER10_GIS_sde_path, "ESA_" + layer)
        #gdb = r"\\besfile1\ccsp\Mapping\Gdb\ESA_WFS_Prod_testing.gdb" #for testing, remove
        #output_fc = os.path.join(gdb, layer) # for testing, remove

        utility.datetime_print("saving to disk at - {}".format(output_fc))
        arcpy.CopyFeatures_management(working_memory, output_fc)

        utility.datetime_print("done")

    except urllib.error.HTTPError as e:
        message = layer + " Failed to write to file"
        print(message)
        print(e.reason)
        print(str(sys.exc_info()[0]))

    except Exception as e:
        arcpy.ExecuteError()
        #log_obj.exception(str(sys.exc_info()[0]))
        #log_obj.info("WFS to GDB Failed".format())

pass