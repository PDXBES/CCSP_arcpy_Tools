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


test_flag = "PROD"
config = config.Config(test_flag)
utility = utility.Utility(config)

# PROD
wfs_url = 'https://geoserver-ccsptools.gov-prod.sitkatech.com/geoserver/CCSP/ows'
# QA
#wfs_url = 'https://geoserver-ccsptools.gov-qa.sitkatech.com/geoserver/CCSP/ows'

# this is the only one giving us an issue
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
        print(layer)
        params = dict(service='WFS', version="1.0.0", request='GetFeature', typeName=layer, outputFormat='json')
        # r = requests.get(wfs_url, params=params, headers=headers, verify=False)
        r = requests.get(wfs_url, params=params, headers=headers)

        if params['outputFormat'] == 'csv':
            text = r.text
            csv = pd.read_csv(io.StringIO(text))
            csv["Geometry"] = csv["Geometry"].apply(wkt.loads)
            data = gpd.GeoDataFrame(csv, geometry="Geometry")
        elif params['outputFormat'] == 'json':
            text = r.text
            out_file = os.path.join(config.json_conversion_temp, layer + ".json")
            f = open(out_file, 'w')
            f.write(text)
            f.close()
            #data = gpd.read_file(io.StringIO(text), geometry="Geometry", driver='GeoJSON')
        else:
            data = None
        # sedf = GeoAccessor.from_geodataframe(data[["LinkID", "Geometry"]])
        # sedf.plot()
        # sedf.spatial.to_featureclass(r'C:\temp_work\working2.gdb/segments', sanitize_columns=False, overwrite=True)
        # output = "c:\\temp\\WFS1\\" + layer + ".json"
        # data.to_file(output, driver='GeoJSON')
    except urllib.error.HTTPError as e:
        message = layer + " Failed to write to file"
        print(message)
        print(e.reason)
        print(str(sys.exc_info()[0]))
pass