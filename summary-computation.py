from pymongo import MongoClient
from bson.son import SON
import datetime

client = MongoClient('mongodb://database/argo')
db = client.argo

# dac summary, response to /argo/dacs
dacs = [
    {
       "$lookup":
         {
           "from": "argo",
           "localField": "_id",
           "foreignField": "metadata",
           "pipeline": [
            {"$project": { "timestamp": 1 }},
            {"$sort": {"timestamp":-1}}
           ],
           "as": "data"
         }
    },
    {
        "$project":{
            "data_center": "$data_center",
            "n": {"$size": "$data"},
            "mostrecent": {"$first": "$data.timestamp"}
        }
    },
    {
        "$group":{
            "_id": "$data_center",
            "n": {"$sum": "$n"},
            "mostrecent": {"$max": "$mostrecent"}
        }
    }
]
dacs = list(db.argoMeta.aggregate(dacs))
try:
    db.summaries.replace_one({"_id": 'argo_dacs'}, {"_id": 'argo_dacs', "summary":dacs}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(dacs)

# bgc summary, response to /argo/bgc
bgc = [
    {
       "$lookup":
         {
           "from": "argo",
           "localField": "_id",
           "foreignField": "metadata",
           "pipeline": [
            {"$match": {"source.source":"argo_bgc"}},
            {"$project": { "timestamp": 1 }},
            {"$sort": {"timestamp":-1}}
           ],
           "as": "data"
         }
    },
    {
        "$project":{
            "platform": "$platform",
            "n": {"$size": "$data"},
            "mostrecent": {"$first": "$data.timestamp"}
        }
    },
    {
        "$group":{
            "_id": "$platform",
            "n": {"$sum": "$n"},
            "mostrecent": {"$max": "$mostrecent"}
        }
    },
    {"$match": {"n":{"$gt":0}}}
]
bgc = list(db.argoMeta.aggregate(bgc))
try:
    db.summaries.replace_one({"_id": 'argo_bgc'}, {"_id": 'argo_bgc', "summary":bgc}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(bgc)

# data_keys enumerations
data_keys = list(db['argo'].distinct('data_info.0'))
data_keys.sort()
try:
    db.summaries.replace_one({"_id": 'argo_data_keys'}, {"_id":'argo_data_keys', "data_keys":data_keys}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(data_keys)

# /argo/overview
argo_overview = {
    "nCore": db.argo.count_documents({"source.source": "argo_core"}),
    "nBGC": db.argo.count_documents({"source.source": "argo_bgc"}),
    "nDeep": db.argo.count_documents({"source.source": "argo_deep"}),
    "mostrecent": list(db.argo.aggregate([{"$sort":{"timestamp":-1}},{"$limit":1}]))[0]['timestamp'],
    "latest_argovis_update": datetime.datetime.now(),
    "datacenters": [x['_id'] for x in dacs]
}

try:
    db.summaries.replace_one({"_id": 'argo_overview'}, {"_id": 'argo_overview', "summary":argo_overview}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(argo_overview)

# json-ld summary
earliest_profile = db.argo.find(
    {"timestamp": {"$lt": datetime.datetime.strptime('2000-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')}},
    {"timestamp": 1}
).sort("timestamp", 1).limit(1)[0]
earliest_year = earliest_profile['timestamp'].year

latest_profile = db.argo.find(
    {"timestamp": {"$gt": datetime.datetime.strptime('2024-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')}},
    {"timestamp": 1}
).sort("timestamp", -1).limit(1)[0]
latest_year = latest_profile['timestamp'].year

northernmost_profile = db.argo.find({
    "geolocation.coordinates": {
        "$geoWithin": {
            "$box": [
                [-180, 80],
                [180, 90]
            ]
        }
    }
}, {"geolocation":1}).sort("geolocation.coordinates.1", -1).limit(1)[0]
northernmost_lat = northernmost_profile['geolocation']['coordinates'][1]

southernmost_profile = db.argo.find({
    "geolocation.coordinates": {
        "$geoWithin": {
            "$box": [
                [-180, -89],  
                [180, -70]
            ]
        }
    }
}, {"geolocation":1}).sort("geolocation.coordinates.1", 1).limit(1)[0]
southernmost_lat = southernmost_profile['geolocation']['coordinates'][1]

data_variables = [
	["bbp470", "Particle backscattering at 470 nanometers", "m-1"],
	["bbp470_argoqc", "QC for bbp470", ""],
	["bbp532", "Particle backscattering at 532 nanometers", "m-1"],
	["bbp532_argoqc", "QC for bbp532", ""],
	["bbp700", "Particle backscattering at 700 nanometers", "m-1"],
	["bbp700_argoqc", "QC for bbp700", ""],
	["bbp700_2", "Particle backscattering at 700 nanometers (second sensor)", "m-1"],
	["bbp700_2_argoqc", "QC for bbp700 (second sensor)", ""],
	["bisulfide", "Bisulfide", "micromole/kg"],
	["bisulfide_argoqc", "QC for bisulfide", ""],
	["cdom", "Concentration of coloured dissolved organic matter in sea water", "ppb"],
	["cdom_argoqc", "QC for cdom", ""],
	["chla", "Chlorophyll-A", "mg/m3"],
	["chla_argoqc", "QC for chla", ""],
	["chla_fluorescence", "Chlorophyll-A signal from fluorescence sensor", "count"],
	["chla_fluorescence_argoqc", "QC for chla_fluorescence", ""],
	["cndc", "Electrical conductivity", "mhos/m"],
	["cndc_argoqc", "QC for cndc", ""],
	["cp660", "Particle beam attenuation at 660 nanometers", "m-1"],
	["cp660_argoqc", "QC for cp660", ""],
	["down_irradiance380", "Downwelling irradiance at 380 nanometers", "W/m^2/nm"],
	["down_irradiance380_argoqc", "QC for down_irradiance380", ""],
	["down_irradiance412", "Downwelling irradiance at 412 nanometers", "W/m^2/nm"],
	["down_irradiance412_argoqc", "QC for down_irradiance412", ""],
	["down_irradiance443", "Downwelling irradiance at 443 nanometers", "W/m^2/nm"],
	["down_irradiance443_argoqc", "QC for down_irradiance443", ""],
	["down_irradiance490", "Downwelling irradiance at 490 nanometers", "W/m^2/nm"],
	["down_irradiance490_argoqc", "QC for down_irradiance490", ""],
	["down_irradiance555", "Downwelling irradiance at 555 nanometers", "W/m^2/nm"],
	["down_irradiance555_argoqc", "QC for down_irradiance555", ""],
	["down_irradiance665", "Downwelling irradiance at 665 nanometers", "W/m^2/nm"],
	["down_irradiance665_argoqc", "QC for down_irradiance665", ""],
	["down_irradiance670", "Downwelling irradiance at 670 nanometers", "W/m^2/nm"],
	["down_irradiance670_argoqc", "QC for down_irradiance670", ""],
	["downwelling_par", "Downwelling photosynthetic available radiation", "microMoleQuanta/m^2/sec"],
	["downwelling_par_argoqc", "QC for downwelling_par", ""],
	["doxy", "Dissolved oxygen", "micromole/kg"],
	["doxy_argoqc", "QC for doxy", ""],
	["doxy2", "Dissolved oxygen (second sensor)", "micromole/kg"],
	["doxy2_argoqc", "QC for doxy2", ""],
	["doxy3", "Dissolved oxygen (third sensor)", "micromole/kg"],
	["doxy3_argoqc", "QC for doxy3", ""],
	["nitrate", "Nitrate", "micromole/kg"],
	["nitrate_argoqc", "QC for nitrate", ""],
	["ph_in_situ_total", "pH", "dimensionless"],
	["ph_in_situ_total_argoqc", "QC for ph_in_situ_total", ""],
	["pressure", "Sea water pressure, equals 0 at sea-level", "decibar"],
	["pressure_argoqc", "QC for pressure", ""],
	["salinity", "Practical salinity", "psu"],
	["salinity_argoqc", "QC for salinity", ""],
	["salinity_sfile", "Practical salinity (reported in BGC synthetic file)", "psu"],
	["salinity_sfile_argoqc", "QC for salinity_sfile", ""],
	["temperature", "Sea temperature in-situ ITS-90 scale", "degree_Celsius"],
	["temperature_argoqc", "QC for temperature", ""],
	["temperature_sfile", "Sea temperature in-situ ITS-90 scale (reported in BGC synthetic file)", "degree_Celsius"],
	["temperature_sfile_argoqc", "QC for temperature_sfile", ""],
	["turbidity", "Sea water turbidity", "ntu"],
	["turbidity_argoqc", "QC for turbidity", ""],
	["up_radiance412", "Upwelling radiance at 412 nanometers", "W/m^2/nm/sr"],
	["up_radiance412_argoqc", "QC for up_radiance412", ""],
	["up_radiance443", "Upwelling radiance at 443 nanometers", "W/m^2/nm/sr"],
	["up_radiance443_argoqc", "QC for up_radiance443", ""],
	["up_radiance490", "Upwelling radiance at 490 nanometers", "W/m^2/nm/sr"],
	["up_radiance490_argoqc", "QC for up_radiance490", ""],
	["up_radiance555", "Upwelling radiance at 555 nanometers", "W/m^2/nm/sr"],
	["up_radiance555_argoqc", "QC for up_radiance555", ""]
]

property_values = [{"@type": "PropertyValue", "name": var[0], "url": "https://archimer.ifremer.fr/doc/00187/29825/94819.pdf", "description": var[1], "unitCode": var[2]} for var in data_variables]

jsonld_summary = {
    "@context": {
        "@vocab": "https://schema.org/"
    },
    "@type": "Dataset",
    "@id": "https://registry.org/permanentUrlToThisJsonDoc",
    "name": "Argovis' representation of the Argo dataset",
    "description": "Argovis provides a representation of the profiles collected over the lifetime of the Argo program. This representation is intended to present an interpretation of Argo data that is lightly simplified from the original product, but still appropriate for a large majority of scientific and educational use cases. Simplifications include presenting delayed (better corrected and QCed) mode data where available; presenting interpolated biogeochemical data only; and merging core and bioogeochemical data collected in parallel into unified oceanic profiles. For an introduction to using the Argovis API to access Argo data in Python, see https://github.com/argovis/demo_notebooks/blob/main/Intro_to_Argovis.ipynb.",
    "url": "https://argovis.colorado.edu/argourlhelper",
    "license": "https://opensource.org/license/mit",
    "citation": [
        "Tucker, T., D. Giglio, M. Scanderbeg, and S.S.P. Shen: Argovis: A Web Application for Fast Delivery, Visualization, and Analysis of Argo Data. J. Atmos. Oceanic Technol., 37, 401–416, https://doi.org/10.1175/JTECH-D-19-0041.1",
        "Wong, A. P. S., et al. (2020), Argo Data 1999–2019: Two Million Temperature-Salinity Profiles and Subsurface Velocity Observations From a Global Array of Profiling Floats, Frontiers in Marine Science, 7(700), doi: https://doi.org/10.3389/fmars.2020.00700",
        "Argo (2000). Argo float data and metadata from Global Data Assembly Centre (Argo GDAC). SEANOE. https://doi.org/10.17882/42182"
    ],
    "creator": [
        {
            "@context": {
                "@vocab": "https://schema.org/"
            },
            "@type": "Person",
            "name": "Donata Giglio",
            "workLocation": {
                "@type": "Place",
                "name": "University of Colorado, Boulder, Dept. of Atmospheric and Ocean Science"
            },
            "identifier": {
                "@id": "https://orcid.org/0000-0002-3738-4293",
                "@type": "PropertyValue",
                "propertyID": "https://registry.identifiers.org/registry/orcid",
                "url": "https://orcid.org/0000-0002-3738-4293",
            }
        },
        {
            "@context": {
                "@vocab": "https://schema.org/"
            },
            "@type": "Person",
            "name": "Bill Katie-Anne Mills",
            "workLocation": {
                "@type": "Place",
                "name": "University of Colorado, Boulder, Dept. of Atmospheric and Ocean Science"
            },
            "identifier": {
                "@id": "https://orcid.org/0000-0002-5887-6270",
                "@type": "PropertyValue",
                "propertyID": "https://registry.identifiers.org/registry/orcid",
                "url": "https://orcid.org/0000-0002-5887-6270",
            }
        },
        {
            "@context": {
                "@vocab": "https://schema.org/"
            },
            "@type": "Person",
            "name": "Megan Scanderbeg",
            "workLocation": {
                "@type": "Place",
                "name": "Scripps Institution of Oceanography"
            },
            "identifier": {
                "@id": "https://orcid.org/0000-0002-0398-7272",
                "@type": "PropertyValue",
                "propertyID": "https://registry.identifiers.org/registry/orcid",
                "url": "https://orcid.org/0000-0002-0398-7272",
            }
        }
    ],
    "version": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
    "keywords": [
        "Argo", 
        "ocean profiles", 
        "temperature", 
        "salinity", 
        "pressure", 
        "ocean biogeochemistry"
    ],
    "measurementTechnique": "http://www.argodatamgt.org/Documentation",
    "variableMeasured": property_values,
    "includedInDataCatalog": {
        "@type": "DataCatalog",
        "url": "https://argovis.colorado.edu/citations"
    },
    "temporalCoverage": f"{earliest_year}/{latest_year}",
    "distribution": {
        "@type": "DataDownload", 
        "url": "https://argovis.colorado.edu/argourlhelper",
        "description": "Argovis provides no direct download of the dataset described in this record as it is too large to download in one click; however, please visit https://argovis.colorado.edu/argourlhelper to dynamically access your own subset of data"
    },
    "spatialCoverage": {
            "@type": "Place",
            "geo": {
                "@type": "GeoShape",
                "box": f"{southernmost_lat} -180 {northernmost_lat} 180"
            },
            "additionalProperty": {
                "@type": "PropertyValue",
                "propertyID": "http://dbpedia.org/resource/Spatial_reference_system",
                "value": "http://www.w3.org/2003/01/geo/wgs84_pos"
            }
    },
    "provider": [
        {
            "@type": "Organization",
            "legalName": "University of Colorado Boulder",
            "name": "Department of Atmospheric and Ocean Science",
            "url": "https://www.colorado.edu/atoc/"
        }
    ]
}

try:
    db.summaries.replace_one({"_id": 'argo_jsonld'}, {"_id": 'argo_jsonld', "jsonld":jsonld_summary}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(jsonld_summary)


# ----- index preheats ------------

poly = {"$geoWithin": {"$geometry": {"type": "Polygon","coordinates": [[[-135,40],[-135,45],[-130,45],[-130,40],[-135,40]]]}}}
time = {"$gte": datetime.datetime.strptime('2018-11-06T00:00:00Z', "%Y-%m-%dT%H:%M:%SZ"), "$lt": datetime.datetime.strptime('2018-11-07T00:00:00Z', "%Y-%m-%dT%H:%M:%SZ")}

preheat = list(db.argo.aggregate([{'$match': {'geolocation': poly}}])) # argo geolocation index
preheat = list(db.argo.aggregate([{'$match': {'timestamp': time}}]))   # argo timestamp index
preheat = list(db.argo.aggregate([{'$match': {'geolocation': poly, 'timestamp':time}}])) # argo timestamp x geolocation index, as optimized by .explain()
preheat = list(db.cchdo.aggregate([{'$match': {'geolocation': poly}}])) # similar for cchdo
preheat = list(db.cchdo.aggregate([{'$match': {'timestamp': time}}]))
preheat = list(db.cchdo.aggregate([{'$match': {'geolocation': poly, 'timestamp':time}}]))



