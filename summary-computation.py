from pymongo import MongoClient
from bson.son import SON
import datetime, json, copy, re

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

# lookup table scraped from http://www.argodatamgt.org/content/download/30910/209488/file/argo-parameters-list-core-and-b_20230612.xlsx
admt_vars = {
	"CNDC" : ["Electrical conductivity", "mhos/m"],
	"PRES" : ["Sea water pressure, equals 0 at sea-level", "decibar"],
	"PSAL" : ["Practical salinity", "psu"],
	"TEMP" : ["Sea temperature in-situ ITS-90 scale", "degree_Celsius"],
	"DOXY" : ["Dissolved oxygen", "micromole/kg"],
	"TEMP_DOXY" : ["Sea temperature from oxygen sensor ITS-90 scale", "degree_Celsius"],
	"TEMP_VOLTAGE_DOXY" : ["Thermistor voltage reported by oxygen sensor", "volt"],
	"VOLTAGE_DOXY" : ["Voltage reported by oxygen sensor", "volt"],
	"FREQUENCY_DOXY" : ["Frequency reported by oxygen sensor", "hertz"],
	"COUNT_DOXY" : ["Count reported by oxygen sensor", "count"],
	"BPHASE_DOXY" : ["Uncalibrated phase shift reported by oxygen sensor", "degree"],
	"DPHASE_DOXY" : ["Calibrated phase shift reported by oxygen sensor", "degree"],
	"TPHASE_DOXY" : ["Uncalibrated phase shift reported by oxygen sensor", "degree"],
	"C1PHASE_DOXY" : ["Uncalibrated phase shift reported by oxygen sensor", "degree"],
	"C2PHASE_DOXY" : ["Uncalibrated phase shift reported by oxygen sensor", "degree"],
	"MOLAR_DOXY" : ["Uncompensated (pressure and salinity) oxygen concentration reported by the oxygen sensor", "micromole/l"],
	"PHASE_DELAY_DOXY" : ["Phase delay reported by oxygen sensor", "microsecond"],
	"MLPL_DOXY" : ["Oxygen concentration reported by the oxygen sensor", "ml/l"],
	"NB_SAMPLE_CTD" : ["Number of samples in each pressure bin for the CTD", "count"],
	"NB_SAMPLE_SFET" : ["Number of samples in each pressure bin for the SFET", "count"],
	"NB_SAMPLE_<parameter_sensor_name>" : ["Number of samples in each pressure bin for the <parameter_sensor_name> ", "count"],
	"RPHASE_DOXY" : ["Uncalibrated red phase shift reported by oxygen sensor", "degree"],
	"TEMP_COUNT_DOXY" : ["Count which is expressive of uncalibrated temperature value reported by oxygen sensor", "count"],
	"LED_FLASHING_COUNT_DOXY" : ["Number of times oxygen sensor flashing to measure oxygen", "count"],
	"PPOX_DOXY" : ["Partial pressure of oxygen", "millibar"],
	"BETA_BACKSCATTERING" : ["Total angle specific volume from backscattering sensor at x nanometers", "count"],
	"BETA_BACKSCATTERING470" : ["Total angle specific volume from backscattering sensor at 470 nanometers", "count"],
	"BETA_BACKSCATTERING532" : ["Total angle specific volume from backscattering sensor at 532 nanometers", "count"],
	"BETA_BACKSCATTERING700" : ["Total angle specific volume from backscattering sensor at 700 nanometers", "count"],
	"FLUORESCENCE_CHLA" : ["Chlorophyll-A signal from fluorescence sensor", "count"],
	"TEMP_CPU_CHLA" : ["Thermistor signal from backscattering sensor", "count"],
	"FLUORESCENCE_CDOM" : ["Raw fluorescence from coloured dissolved organic matter sensor", "count"],
	"SIDE_SCATTERING_TURBIDITY" : ["Turbidity signal from side scattering sensor", "count"],
	"TRANSMITTANCE_PARTICLE_BEAM_ATTENUATION" : ["Beam attenuation from transmissometer sensor at x nanometers", "count"],
	"TRANSMITTANCE_PARTICLE_BEAM_ATTENUATION660" : ["Beam attenuation from transmissometer sensor at 660 nanometers", "count"],
	"BBP" : ["Particle backscattering at x nanometers", "m-1"],
	"BBP470" : ["Particle backscattering at 470 nanometers", "m-1"],
	"BBP532" : ["Particle backscattering at 532 nanometers", "m-1"],
	"BBP700" : ["Particle backscattering at 700 nanometers", "m-1"],
	"TURBIDITY" : ["Sea water turbidity", "ntu"],
	"CP" : ["Particle beam attenuation at x nanometers", "m-1"],
	"CP660" : ["Particle beam attenuation at 660 nanometers", "m-1"],
	"CHLA" : ["Chlorophyll-A", "mg/m3"],
	"CDOM" : ["Concentration of coloured dissolved organic matter in sea water", "ppb"],
	"UV_INTENSITY_NITRATE" : ["Intensity of ultra violet flux from nitrate sensor", "count"],
	"UV_INTENSITY_DARK_NITRATE" : ["Intensity of ultra violet flux dark measurement from nitrate sensor", "count"],
	"UV_INTENSITY_DARK_SEAWATER_NITRATE" : ["Intensity of ultra-violet flux dark sea water from nitrate sensor", "count"],
	"NITRATE" : ["Nitrate", "micromole/kg"],
	"BISULFIDE" : ["Bisulfide", "micromole/kg"],
	"MOLAR_NITRATE" : ["Nitrate", "micromole/l"],
	"FIT_ERROR_NITRATE" : ["Nitrate fit error", "dimensionless"],
	"TEMP_NITRATE" : ["Internal temperature of the SUNA sensor", "degree_Celsius"],
	"TEMP_SPECTROPHOTOMETER_NITRATE" : ["Temperature of the spectrometer", "degree_Celsius"],
	"HUMIDITY_NITRATE" : ["Relative humidity inside the SUNA sensor (If > 50% There is a leak)", "percent"],
	"VRS_PH" : ["Voltage difference between reference and source from pH sensor", "volt"],
	"TEMP_PH" : ["Sea temperature from pH sensor", "degree_Celsius"],
	"IB_PH" : ["Base current of pH sensor", "nanoampere"],
	"VK_PH" : ["Counter electrode voltage of pH sensor", "volt"],
	"IK_PH" : ["Counter electrode current of pH sensor", "nanoampere"],
	"PH_IN_SITU_TOTAL" : ["pH", "dimensionless"],
	"PH_IN_SITU_FREE" : ["pH", "dimensionless"],
	"PH_IN_SITU_SEAWATER" : ["pH", "dimensionless"],
	"RAW_DOWNWELLING_IRRADIANCE" : ["Raw downwelling irradiance at x nanometers", "count"],
	"RAW_DOWNWELLING_IRRADIANCE380" : ["Raw downwelling irradiance at 380 nanometers", "count"],
	"RAW_DOWNWELLING_IRRADIANCE412" : ["Raw downwelling irradiance at 412 nanometers", "count"],
	"RAW_DOWNWELLING_IRRADIANCE443" : ["Raw downwelling irradiance at 443 nanometers", "count"],
	"RAW_DOWNWELLING_IRRADIANCE490" : ["Raw downwelling irradiance at 490 nanometers", "count"],
	"RAW_DOWNWELLING_IRRADIANCE555" : ["Raw downwelling irradiance at 555 nanometers", "count"],
	"RAW_DOWNWELLING_IRRADIANCE665" : ["Raw downwelling irradiance at 665 nanometers", "count"],
	"RAW_DOWNWELLING_IRRADIANCE670" : ["Raw downwelling irradiance at 670 nanometers", "count"],
	"DOWN_IRRADIANCE" : ["Downwelling irradiance at x nanometers", "W/m^2/nm"],
	"DOWN_IRRADIANCE380" : ["Downwelling irradiance at 380 nanometers", "W/m^2/nm"],
	"DOWN_IRRADIANCE412" : ["Downwelling irradiance at 412 nanometers", "W/m^2/nm"],
	"DOWN_IRRADIANCE443" : ["Downwelling irradiance at 443 nanometers", "W/m^2/nm"],
	"DOWN_IRRADIANCE490" : ["Downwelling irradiance at 490 nanometers", "W/m^2/nm"],
	"DOWN_IRRADIANCE555" : ["Downwelling irradiance at 555 nanometers", "W/m^2/nm"],
	"DOWN_IRRADIANCE665" : ["Downwelling irradiance at 665 nanometers", "W/m^2/nm"],
	"DOWN_IRRADIANCE670" : ["Downwelling irradiance at 670 nanometers", "W/m^2/nm"],
	"RAW_UPWELLING_RADIANCE" : ["Raw upwelling radiance at x nanometers", "count"],
	"RAW_UPWELLING_RADIANCE412" : ["Raw upwelling radiance at 412 nanometers", "count"],
	"RAW_UPWELLING_RADIANCE443" : ["Raw upwelling radiance at 443 nanometers", "count"],
	"RAW_UPWELLING_RADIANCE490" : ["Raw upwelling radiance at 490 nanometers", "count"],
	"RAW_UPWELLING_RADIANCE555" : ["Raw upwelling radiance at 555 nanometers", "count"],
	"UP_RADIANCE" : ["Upwelling radiance at x nanometers", "W/m^2/nm/sr"],
	"UP_RADIANCE412" : ["Upwelling radiance at 412 nanometers", "W/m^2/nm/sr"],
	"UP_RADIANCE443" : ["Upwelling radiance at 443 nanometers", "W/m^2/nm/sr"],
	"UP_RADIANCE490" : ["Upwelling radiance at 490 nanometers", "W/m^2/nm/sr"],
	"UP_RADIANCE555" : ["Upwelling radiance at 555 nanometers", "W/m^2/nm/sr"],
	"RAW_DOWNWELLING_PAR" : ["Raw downwelling photosynthetic available radiation", "count"],
	"DOWNWELLING_PAR" : ["Downwelling photosynthetic available radiation", "microMoleQuanta/m^2/sec"],
	"TILT" : ["Inclination of the float axis in respect to the local vertical", "degree"],
	"MTIME" : ["Fractional day of the individual measurement relative to JULD of the station", "days"],
	"TEMP_CNDC" : ["Internal temperature of the conductivity cell", "degree_Celsius"],
	"CHLA_FLUORESCENCE" : ["Chlorophyll fluorescence with factory calibration", "ru"]
} 

property_values = []
for k in data_keys:
    var = ["variable not found in ADMT documentation", ""]
    isQC = k.endswith("_argoqc")
    isSfile = k.endswith("_sfile")
    varname = re.sub('_argoqc$', '', k)
    argoname = varname.upper()
    argovis_mappings = {
        'TEMPERATURE': 'TEMP',
        'TEMPERATURE_SFILE': 'TEMP',
        'SALINITY': 'PSAL',
        'SALINITY_SFILE': 'PSAL',
        'PRESSURE': 'PRES'
    }
    if argoname in argovis_mappings:
        argoname = argovis_mappings[argoname]

    if argoname in admt_vars:
        var = admt_vars[argoname].copy()
        if isSfile:
            var[0] += ' from BGC file'
        if isQC:
            var = [f"QC for {varname}", ""]
    elif varname.endswith(tuple(str(i) for i in range(10))):
        sensor = int(varname[-1:])
        varname = varname[:-1]
        argoname = varname.rstrip('_').upper()
        if argoname in argovis_mappings:
            argoname = argovis_mappings[argoname]
        if argoname in admt_vars:
            var = admt_vars[argoname].copy()
            var[0] += f" (sensor {sensor})"
            if isSfile:
                var[0] += ' from BGC file'
            if isQC:
                var = [f"QC for {varname}{sensor}", ""]       
    property_values.append({"@type": "PropertyValue", "name": k, "url": "https://archimer.ifremer.fr/doc/00187/29825/94819.pdf", "description": var[0], "unitCode": var[1]})

jsonld_summary = {
    "@context": {
        "@vocab": "https://schema.org/"
    },
    "@type": "Dataset",
    "@id": "https://argovis-api.colorado.edu/summary?id=argo_jsonld&key=jsonld",
    "name": "Argovis' representation of the Argo dataset",
    "description": "Argovis provides a representation of the profiles collected over the lifetime of the Argo program. This representation is intended to present an interpretation of Argo data that is slightly simplified from the original product, but still appropriate for a large majority of scientific and educational use cases. Simplifications include presenting delayed (better corrected and QCed) mode data where available; presenting interpolated biogeochemical data only; and merging core and biogeochemical data collected in parallel into unified oceanic profiles. For an introduction to using the Argovis API to access Argo data see https://argovis.colorado.edu/argourlhelper. If using Python, please also see https://github.com/argovis/demo_notebooks/blob/main/Intro_to_Argovis.ipynb. Argo data in Argovis can be integrated (via the Argovis API) into living documents like Jupyter notebooks and analyses intended to update their consumption of Argo data in near-real-time, and our web frontend (https://argovis.colorado.edu/argo) is intended to make it easy for students and educators to explore Argo data at will. Finally, Argovis API also makes it easy to co-locate Argo profiles with other datasets.",
    "url": "https://argovis.colorado.edu/argourlhelper",
    "license": "https://opensource.org/license/mit",
    "creditText": [
        "Tucker, T., D. Giglio, M. Scanderbeg, and S.S.P. Shen: Argovis: A Web Application for Fast Delivery, Visualization, and Analysis of Argo Data. J. Atmos. Oceanic Technol., 37, 401–416, https://doi.org/10.1175/JTECH-D-19-0041.1"
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
    "sdPublisher": [
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

# rate limiter metadata

def get_timestamp_range(db, collection_name):
    collection = db[collection_name]
    
    # Find the earliest timestamp
    filter = {}
    if 'qc' in datasets[collection_name]:
        filter[datasets[collection_name]['qc']] = 1
    earliest_doc = collection.find_one(filter, sort=[("timestamp", 1)])
    if earliest_doc and "timestamp" in earliest_doc:
        earliest_timestamp = earliest_doc["timestamp"]
    else:
        return None, None  # Return None if no timestamps are found

    # Find the latest timestamp or current time, whichever is earlier
    filter = {}
    if 'qc' in datasets[collection_name]:
        filter[datasets[collection_name]['qc']] = 1
    latest_doc = collection.find_one(filter, sort=[("timestamp", -1)])
    current_time = datetime.datetime.utcnow()

    if latest_doc and "timestamp" in latest_doc:
        latest_timestamp = min(latest_doc["timestamp"], current_time)
    else:
        latest_timestamp = current_time  # If no documents, default to current time

    # Convert timestamps to ISO 8601 format
    try:
        earliest_iso = earliest_timestamp.isoformat() + "Z"
        latest_iso = latest_timestamp.isoformat() + "Z"
        return earliest_iso, latest_iso
    except:
        return None, None

datasets = {
    # metagroups: indexed fields to allow rate limiter cost discounts for; corresponds more or less to the special fields listed in each dataset's service's local_filter and metafilter
    'ar': {'metagroups': ['_id'], 'startDate': None, 'endDate': None},
    'argo': {'metagroups': ['_id', 'metadata', 'platform'], 'startDate': None, 'endDate': None, 'qc': 'timestamp_argoqc'},
    #'argone': {'metagroups': ['_id'], 'startDate': None, 'endDate': None}, # doesn't have timestamps
    'cchdo': {'metagroups': ['_id', 'metadata', 'woceline', 'cchdo_cruise'], 'startDate': None, 'endDate': None},
    #'drifter': {'metagroups': ['_id', 'metadata', 'wmo', 'platform'], 'startDate': None, 'endDate': None}, # drifters live in an independent deployment, do this over there
    'easyocean': {'metagroups': ['_id'], 'startDate': None, 'endDate': None},
    'rg09': {'metagroups': ['_id'], 'startDate': None, 'endDate': None},
    'kg21': {'metagroups': ['_id'], 'startDate': None, 'endDate': None},
    'glodap': {'metagroups': ['_id'], 'startDate': None, 'endDate': None},
    'tc': {'metagroups': ['_id', 'metadata', 'name'], 'startDate': None, 'endDate': None},
    'noaasst': {'metagroups': ['_id'], 'startDate': None, 'endDate': None},
    'copernicussla': {'metagroups': ['_id'], 'startDate': None, 'endDate': None},
    'ccmpwind': {'metagroups': ['_id'], 'startDate': None, 'endDate': None},
    'argotrajectories': {'metagroups': ['_id', 'metadata', 'platform'], 'startDate': None, 'endDate': None},
}

timeseries = ['noaasst', 'copernicussla', 'ccmpwind']

for dataset in datasets:
    if dataset in timeseries:
        ts = db['timeseriesMeta'].find_one({"_id":dataset})['timeseries']
        startDate = ts[0].isoformat() + "Z"
        endDate = ts[-1].isoformat() + "Z"
    else:
        startDate, endDate = get_timestamp_range(db, dataset)
    datasets[dataset]['startDate'] = startDate
    datasets[dataset]['endDate'] = endDate

try:
    db.summaries.replace_one({"_id": 'ratelimiter'}, {"_id": 'ratelimiter', "metadata":datasets}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(datasets)

# ----- index preheats ------------

poly = {"$geoWithin": {"$geometry": {"type": "Polygon","coordinates": [[[-135,40],[-135,45],[-130,45],[-130,40],[-135,40]]]}}}
time = {"$gte": datetime.datetime.strptime('2018-11-06T00:00:00Z', "%Y-%m-%dT%H:%M:%SZ"), "$lt": datetime.datetime.strptime('2018-11-07T00:00:00Z', "%Y-%m-%dT%H:%M:%SZ")}

preheat = list(db.argo.aggregate([{'$match': {'geolocation': poly}}])) # argo geolocation index
preheat = list(db.argo.aggregate([{'$match': {'timestamp': time}}]))   # argo timestamp index
preheat = list(db.argo.aggregate([{'$match': {'geolocation': poly, 'timestamp':time}}])) # argo timestamp x geolocation index, as optimized by .explain()
preheat = list(db.cchdo.aggregate([{'$match': {'geolocation': poly}}])) # similar for cchdo
preheat = list(db.cchdo.aggregate([{'$match': {'timestamp': time}}]))
preheat = list(db.cchdo.aggregate([{'$match': {'geolocation': poly, 'timestamp':time}}]))



