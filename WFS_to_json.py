import geopandas as gpd
import pandas as pd
import requests
from shapely import wkt
import sys


#wfs_url = 'https://geoserver-ccsptools.gov-prod.sitkatech.com/geoserver/CCSP/ows'
wfs_url = 'https://geoserver-ccsptools.gov-qa.sitkatech.com/geoserver/CCSP/ows'

layers = [# "CCSPCharacterizationAreas",
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
          # "RehabNodes",
          # "RehabReportLinkFMEs",
          # "RehabReportLinks",
          # "RehabReportRULLinkSegments",
            "RehabReportMortalityCOFLinkSegments",
          # "RehabReportPipXPLinkSegments",

          # "DashboardForecastingReportAreas",
          # "DashboardForecastingReportBlockObjects",
          # "DashboardForecastingReportLinks",
          # "DashboardForecastingReportMAUs",
          # "DashboardForecastingReportNodes",
          # "DashboardForecastingReportProjects",
          # "DashboardForecastingReportSewerBasins"
         ]

for layer in layers:
    try:
        print(layer)
        params = dict(service='WFS', version="1.0.0", request='GetFeature', typeName=layer, outputFormat='csv')
        r = requests.Request('GET', wfs_url, params=params).prepare()
        csv = pd.read_csv(r.url)
        csv["Geometry"] = csv["Geometry"].apply(wkt.loads)
        #csv.apply(pd.numeric(downcast="signed", errors="ignore"))
        data = gpd.GeoDataFrame(csv, geometry="Geometry")
        output = "c:\\temp\\WFS\\" + layer + ".json"
        data.to_file(output, driver='GeoJSON')
    except:
        message = layer + " Failed to write to file"
        print(message)
        print(str(sys.exc_info()[0]))
pass