import re, xarray, datetime, math
from geopy import distance

def pickprof(filename):
    # identify the profile number from a filename
    # allow a D suffix for decending profiles
    m = re.search('_([0-9]*D{0,1})', filename)
    if m:
        return m.group(1)
    else:
        print('error: failed to find sensible profile number from ' + filename)
        return None

def choose_prefix(prefixes):
    # given a list of nc file prefixes from the set:
    # SD synth delayed
    # SR synth realtime
    # BD bgc delayed
    # BR bgc realtime
    # D  core delayed
    # R  core realtime
    # return which should be chosen as best files to use, based on:
    #     For BGC data: SD always preferred, SR second-preferred, BD and BR always discarded
    #     For core data: D preferred, R second-preferred

    if not set(prefixes).issubset(['SD', 'SR', 'BD', 'BR', 'D', 'R']):
        print('error: nonstandard prefix found in list: ' + prefixes)
        return None

    pfx = []
    # choose synth / BGC data
    if 'SD' in prefixes:
        pfx = ['SD']
    elif 'SR' in prefixes:
        pfx = ['SR']
    # choose core data
    if 'D' in prefixes:
        pfx.append('D')
    elif 'R' in prefixes:
        pfx.append('R')
    return pfx

def argo_keymapping(nckey):
    # argo netcdf measurement name -> argovis measurement name

    key_mapping = {
        "BBP470": "bbp470",
        "BBP532": "bbp532",
        "BBP700": "bbp700",
        "BBP700_2": "bbp700_2",
        "BISULFIDE": "bisulfide",
        "CDOM": "cdom",
        "CHLA": "chla",
        "CNDX": "cndx",
        "CP660": "cp660",
        "DOWN_IRRADIANCE380": "down_irradiance380",
        "DOWN_IRRADIANCE412": "down_irradiance412",
        "DOWN_IRRADIANCE442": "down_irradiance442",
        "DOWN_IRRADIANCE443": "down_irradiance443",
        "DOWN_IRRADIANCE490": "down_irradiance490",
        "DOWN_IRRADIANCE555": "down_irradiance555",
        "DOWN_IRRADIANCE670": "down_irradiance670",
        "DOWNWELLING_PAR": "downwelling_par",
        "DOXY": "doxy",
        "DOXY2": "doxy2",
        "MOLAR_DOXY": "molar_doxy",
        "NITRATE": "nitrate",
        "PH_IN_SITU_TOTAL": "ph_in_situ_total",
        "PRES": "pres",
        "PSAL": "psal",
        "TEMP": "temp",
        "TURBIDITY": "turbidity",
        "UP_RADIANCE412": "up_radiance412",
        "UP_RADIANCE443": "up_radiance443",
        "UP_RADIANCE490": "up_radiance490",
        "UP_RADIANCE555": "up_radiance555",
        "BBP470_QC": "bbp470_argoqc",
        "BBP532_QC": "bbp532_argoqc",
        "BBP700_QC": "bbp700_argoqc",
        "BBP700_2_QC": "bbp700_2_argoqc",
        "BISULFIDE_QC": "bisulfide_argoqc",
        "CDOM_QC": "cdom_argoqc",
        "CHLA_QC": "chla_argoqc",
        "CNDX_QC": "cndx_argoqc",
        "CP660_QC": "cp660_argoqc",
        "DOWN_IRRADIANCE380_QC": "down_irradiance380_argoqc",
        "DOWN_IRRADIANCE412_QC": "down_irradiance412_argoqc",
        "DOWN_IRRADIANCE442_QC": "down_irradiance442_argoqc",
        "DOWN_IRRADIANCE443_QC": "down_irradiance443_argoqc",
        "DOWN_IRRADIANCE490_QC": "down_irradiance490_argoqc",
        "DOWN_IRRADIANCE555_QC": "down_irradiance555_argoqc",
        "DOWN_IRRADIANCE670_QC": "down_irradiance670_argoqc",
        "DOWNWELLING_PAR_QC": "downwelling_par_argoqc",
        "DOXY_QC": "doxy_argoqc",
        "DOXY2_QC": "doxy2_argoqc",
        "MOLAR_DOXY_QC": "molar_doxy_argoqc",
        "NITRATE_QC": "nitrate_argoqc",
        "PH_IN_SITU_TOTAL_QC": "ph_in_situ_total_argoqc",
        "PRES_QC": "pres_argoqc",
        "PSAL_QC": "psal_argoqc",
        "TEMP_QC": "temp_argoqc",
        "TURBIDITY_QC": "turbidity_argoqc",
        "UP_RADIANCE412_QC": "up_radiance412_argoqc",
        "UP_RADIANCE443_QC": "up_radiance443_argoqc",
        "UP_RADIANCE490_QC": "up_radiance490_argoqc",
        "UP_RADIANCE555_QC": "up_radiance555_argoqc"

    }

    try:
        argoname = key_mapping[nckey.replace('_ADJUSTED', '')]
    except:
        print('warning: unexpected variable found in station_parameters:', nckey)
        argoname = nckey.replace('_ADJUSTED', '').replace('_QC', '_argoqc').lower()

    return argoname

def pack_objects(measurements):
    # given an object measurements with keys==variable names (temp, temp_argoqc, pres...) and values equal to depth-ordered lists of the corresponding data,
    # return a depth-ordered list of objects with the appropriate keys.

    ## sanity checks
    if "PRES" not in measurements.keys():
        print('error: measurements objects must have a PRES key.')
        return None
    nLevels = len(measurements['PRES'])
    for var in measurements.keys():
        if len(measurements[var]) != nLevels:
            print('error: measurements', var, 'doesnt have the same number of levels as the provided PRES entry')
            return None
        if var[-3:] != '_QC' and var+'_QC' not in measurements.keys():
            print('error: measurements', var, 'doesnt include a QC vector', var[-3:])

    repack = []
    for i in range(nLevels):
        level = {}
        for key in measurements.keys():
            level[argo_keymapping(key)] = measurements[key][i]
        repack.append(level)

    return repack

def stringcycle(cyclenumber):
    # given a numerical cyclenumber,
    # return a string left padded with 0s appropriate for use in a profile ID

    c = int(cyclenumber)
    if c < 10:
        return '00'+str(c)
    elif c < 100:
        return '0'+str(c)
    else:
        return str(c)

def extract_metadata(ncfile, pidx=0):
    # given the path ncfile to an argo nc file,
    # extract and return a dictionary representing the metadata of the pidx'th profile in that file

    # some helpful facts and figures
    metadata = {}
    data_warning = []
    xar = xarray.open_dataset(ncfile)
    REprefix = re.compile('^[A-Z]*')  
    prefix = REprefix.search(ncfile.split('/')[-1]).group(0)
    variables = list(xar.variables)

    # parse location
    LONGITUDE, LATITUDE = parse_location(xar['LONGITUDE'].to_dict()['data'][pidx], xar['LATITUDE'].to_dict()['data'][pidx])
    if LATITUDE == -90 and LONGITUDE == 0:
        data_warning.append('missing_location')

    ## platform_id
    metadata['platform_id'] = xar['PLATFORM_NUMBER'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## cycle_number
    metadata['cycle_number'] = int(xar['CYCLE_NUMBER'].to_dict()['data'][pidx])

    ## profile_direction
    if('DIRECTION') in variables:
        metadata['profile_direction'] = xar['DIRECTION'].to_dict()['data'][pidx].decode('UTF-8') 

    # id == platform_cycle<D> for primary profile
    metadata['_id'] = str(metadata['platform_id']) + '_' + stringcycle(metadata['cycle_number'])
    if metadata['profile_direction'] == 'D':
        metadata['_id'] += 'D'

    ## basin
    if 'missing_location' not in data_warning:
        metadata['basin'] = find_basin(LONGITUDE, LATITUDE)
        if metadata['basin'] == -1:
            data_warning.append('missing_basin')
    else:
        metadata['basin'] = -1

    ## data_type
    metadata['data_type'] = 'oceanicProfile'

    ## doi: TODO

    ## geolocation
    metadata['geolocation'] = {"type": "Point", "coordinates": [LONGITUDE, LATITUDE]}

    ## instrument
    metadata['instrument'] = 'profiling_float'

    ## source_info
    metadata['source_info'] = [{}]

    ### source_info.source
    isDeep = False
    deepthresh = 2500
    if prefix in ['R', 'D']:
        # core argo
        metadata['source_info'][0]['source'] = ['argo_core']
        #### deep
        DATA_MODE = xar['DATA_MODE'].to_dict()['data'][pidx].decode('UTF-8')
        if DATA_MODE in ['A', 'D']:
            isDeep = max(xar['PRES_ADJUSTED'].to_dict()['data'][pidx]) > deepthresh
        elif DATA_MODE == 'R':
            isDeep = max(xar['PRES'].to_dict()['data'][pidx]) > deepthresh
    elif prefix in ['SR', 'SD']:
        # bgc argo
        metadata['source_info'][0]['source'] = ['argo_bgc']
        #### deep
        PARAMETER_DATA_MODE = [x.decode('UTF-8') for x in xar['PARAMETER_DATA_MODE'].to_dict()['data'][pidx]]
        STATION_PARAMETERS = [x.decode('UTF-8').strip() for x in xar['STATION_PARAMETERS'].to_dict()['data'][pidx]]
        pressure_mode = PARAMETER_DATA_MODE[STATION_PARAMETERS.index('PRES')]
        if pressure_mode in ['D', 'A']:
            isDeep = max(xar['PRES_ADJUSTED'].to_dict()['data'][pidx]) > deepthresh
        elif pressure_mode == 'R':
            isDeep = max(xar['PRES'].to_dict()['data'][pidx]) > deepthresh

    if isDeep:
        metadata['source_info'][0]['source'].append('argo_deep')

    ### source_info.source_url
    metadata['source_info'][0]['source_url'] = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/' + ncfile[9:]

    ### source_info.date_updated_source
    metadata['source_info'][0]['date_updated_source'] = datetime.datetime.strptime(xar['DATE_UPDATE'].to_dict()['data'].decode('UTF-8'),'%Y%m%d%H%M%S')

    ### source_info.data_keys_source
    metadata['source_info'][0]['data_keys_source'] = [key.decode('UTF-8').strip() for key in xar['STATION_PARAMETERS'].to_dict()['data'][pidx]]

    ## data_center
    if('DATA_CENTRE') in variables:
      metadata['data_center'] = xar['DATA_CENTRE'].to_dict()['data'][pidx].decode('UTF-8')

    ## timestamp: 
    metadata['timestamp'] = xar['JULD'].to_dict()['data'][pidx]
    if metadata['timestamp'] is None:
        metadata['timestamp'] = datetime.datetime(9999, 1, 1)
        data_warning.append('missing_timestamp')

    ## date_updated_argovis
    metadata['date_updated_argovis'] = datetime.datetime.now()

    ## pi_name
    if('PI_NAME') in variables:
        metadata['pi_name'] = xar['PI_NAME'].to_dict()['data'][pidx].decode('UTF-8').strip().split(',')

    ## country: TODO

    ## geolocation_argoqc
    if('POSITION_QC') in variables:
        try:
            metadata['geolocation_argoqc'] = int(xar['POSITION_QC'].to_dict()['data'][pidx].decode('UTF-8'))
        except:
            metadata['geolocation_argoqc'] = -1

    ## timestamp_argoqc
    if('JULD_QC') in variables:
        try:
            metadata['timestamp_argoqc'] = int(xar['JULD_QC'].to_dict()['data'][pidx].decode('UTF-8'))
        except:
            metadata['timestamp_argoqc'] = -1

    ## fleetmonitoring
    metadata['fleetmonitoring'] = 'https://fleetmonitoring.euro-argo.eu/float/' + str(metadata['platform_id'])

    ## oceanops
    metadata['oceanops'] = 'https://www.ocean-ops.org/board/wa/Platform?ref=' + str(metadata['platform_id'])

    ## platform_type
    if('PLATFORM_TYPE') in variables:
      metadata['platform_type'] = xar['PLATFORM_TYPE'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## positioning_system
    if('POSITIONING_SYSTEM') in variables:
      metadata['positioning_system'] = xar['POSITIONING_SYSTEM'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## vertical_sampling_scheme
    if('VERTICAL_SAMPLING_SCHEME') in variables:
      metadata['vertical_sampling_scheme'] = xar['VERTICAL_SAMPLING_SCHEME'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## wmo_inst_type
    if('WMO_INST_TYPE') in variables:
      metadata['wmo_inst_type'] = xar['WMO_INST_TYPE'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## data_warning
    if len(data_warning) > 0:
        metadata['data_warning'] = data_warning

    xar.close()
    return metadata

def find_basin(lon, lat):
    # for a given lon, lat,
    # identify the basin from the lookup table.
    # choose the nearest non-nan grid point.

    gridspacing = 0.5
    basins = xarray.open_dataset('parameters/basinmask_01.nc')

    basin = basins['BASIN_TAG'].sel(LONGITUDE=lon, LATITUDE=lat, method="nearest").to_dict()['data']
    if math.isnan(basin):
        # nearest point was on land - find the nearest non nan instead.
        lonplus = math.ceil(lon / gridspacing)*gridspacing
        lonminus = math.floor(lon / gridspacing)*gridspacing
        latplus = math.ceil(lat / gridspacing)*gridspacing
        latminus = math.floor(lat / gridspacing)*gridspacing
        grids = [(basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonplus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonplus)).miles)]

        grids = [x for x in grids if not math.isnan(x[0])]
        if len(grids) == 0:
            # all points on land
            print('warning: all surrounding basin grid points are NaN')
            basin = -1
        else:
            grids.sort(key=lambda tup: tup[1])
            basin = grids[0][0]
    basins.close()
    return int(basin)


def compare_metadata(metadata):
    # given a list of metadata objects as returned by extract_metadata,
    # return true if all list elements are mutually consistent with having come from the same profile

    comparisons = ['platform_id', 'cycle_number', '_id', 'basin', 'data_type', 'geolocation', 'instrument', 'data_center', 'timestamp', 'pi_name', 'geolocation_argoqc', 'timestamp_argoqc', 'fleetmonitoring', 'oceanops', 'platform_type', 'positioning_system', 'vertical_sampling_scheme', 'wmo_inst_type']

    for m in metadata[1:]:
        for c in comparisons:
            if c in metadata[0] and c in m:
                if metadata[0][c] != m[c]:
                    print(metadata[0][c], m[c])
                    return False
                elif (c in metadata[0] and c not in m) or (c not in metadata[0] and c in m):
                    return False

    return True

def extract_data(ncfile, pidx=0):
    # given the path ncfile to an argo nc file,
    # extract and return an object with:
    # data_keys: list of data names found in the pidx'th profile in that file,
    # data: a level-ordered list of lists of the data values, in the same order as data_keys,
    # data_keys_mode: a dict keyed by non-QC variables found in data_keys, with values of 'D'elayed, 'A'djusted, or 'R'ealtime indicating the mode of each variable
    # ie: {data_keys: ['pres', 'pres_argoqc', 'temp', 'temp_argoqc'], data: [[0.0, 1, 23.4, 1], [1.5, 1, 20.1, 1], ....]}
    # return None if nonsense detected

    # some helpful facts and figures
    xar = xarray.open_dataset(ncfile)
    REprefix = re.compile('^[A-Z]*')  
    prefix = REprefix.search(ncfile.split('/')[-1]).group(0)
    allowed_core = ['PRES', 'TEMP', 'PSAL'] # will only consider these variables in core files, anything else should be ignored

    if prefix in ['D', 'R']:
        # core profile
        if 'DATA_MODE' not in list(xar.variables):
            print('error: DATA_MODE not found.')
            return None
        DATA_MODE = xar['DATA_MODE'].to_dict()['data'][pidx].decode('UTF-8')
        if DATA_MODE in ['A', 'D']:
            # use adjusted data
            if 'PRES_ADJUSTED' not in list(xar.variables):
                print('error: no PRES_ADJUSTED found')
                return None
            ## translate the STATION_PARAMETERS into [<PAR>_ADJUSTED, <PAR>_ADJUSTED_QC, ...
            data_sought = [f(x) for x in xar['STATION_PARAMETERS'].to_dict()['data'][pidx] if x.decode('UTF-8').strip() in allowed_core for f in (lambda name: name.decode('UTF-8').strip()+'_ADJUSTED',lambda name: name.decode('UTF-8').strip()+'_ADJUSTED_QC')]
            nc_pressure = xar['PRES_ADJUSTED'].to_dict()['data'][pidx]
        elif DATA_MODE == 'R':
            # use unadjusted data
            if 'PRES' not in list(xar.variables):
                print('error: no PRES found')
                return None
            data_sought = [f(x) for x in xar['STATION_PARAMETERS'].to_dict()['data'][pidx] if x.decode('UTF-8').strip() in allowed_core for f in (lambda name: name.decode('UTF-8').strip(),lambda name: name.decode('UTF-8').strip()+'_QC')]
            nc_pressure = xar['PRES'].to_dict()['data'][pidx]
        else:
            print('error: unexpected data mode detected:', DATA_MODE)
        degenerate_levels = len(nc_pressure) != len(set(nc_pressure)) # known error: profiles with repeated pressures in the same file
        data_by_var = [xar[x].to_dict()['data'][pidx] for x in data_sought]
        argokeys = [argo_keymapping(x) for x in data_sought]
        data_keys_mode = {k: DATA_MODE for k in argokeys if '_argoqc' not in k} # ie assign the global mode to all non qc variables
        data_by_level = [list(x) for x in zip(*data_by_var)]
        data_by_level = [x for x in data_by_level if not math.isnan(x[argokeys.index('pres')])] # ie each level must have a pressure measurement
        return {"data_keys": argokeys, "data": data_by_level, "data_keys_mode": data_keys_mode, "data_annotation": {"degenerate_levels": degenerate_levels, "argo_deep": max(nc_pressure)>2500}}

    elif prefix in ['SD', 'SR']:
        # BGC profile
        if 'PARAMETER_DATA_MODE' not in list(xar.variables):
            print('error: PARAMETER_DATA_MODE not found.')
            return None
        PARAMETER_DATA_MODE = [x.decode('UTF-8') for x in xar['PARAMETER_DATA_MODE'].to_dict()['data'][pidx]]
        STATION_PARAMETERS = [x.decode('UTF-8').strip() for x in xar['STATION_PARAMETERS'].to_dict()['data'][pidx]]
        data_sought = []
        data_keys_mode = {}
        for var in zip(PARAMETER_DATA_MODE, STATION_PARAMETERS):
            if var[0] in ['D', 'A']:
                # use adjusted data
                data_sought.extend([var[1]+'_ADJUSTED', var[1]+'_ADJUSTED_QC'])
                data_keys_mode[argo_keymapping(var[1]).replace('temp', 'temp_sfile').replace('psal', 'psal_sfile')] = var[0]
                nc_pressure = xar['PRES_ADJUSTED'].to_dict()['data'][pidx]
            elif var[0] == 'R':
                # use unadjusted data
                data_sought.extend([var[1],var[1]+'_QC'])
                data_keys_mode[argo_keymapping(var[1]).replace('temp', 'temp_sfile').replace('psal', 'psal_sfile')] = var[0]
                nc_pressure = xar['PRES'].to_dict()['data'][pidx]
            else:
                print('error: unexpected data mode detected for', var[1])
        degenerate_levels = len(nc_pressure) != len(set(nc_pressure)) 
        data_by_var = [xar[x].to_dict()['data'][pidx] for x in data_sought]
        argokeys = [argo_keymapping(x).replace('temp', 'temp_sfile').replace('psal', 'psal_sfile') for x in data_sought]
        data_by_level = [list(x) for x in zip(*data_by_var)]
        data_by_level = [x for x in data_by_level if not math.isnan(x[argokeys.index('pres')])] 
        return {"data_keys": argokeys, "data": data_by_level, "data_keys_mode": data_keys_mode,  "data_annotation": {"degenerate_levels": degenerate_levels, "argo_deep": max(nc_pressure)>2500} }

    else:
        print('error: got unexpected prefix when extracting data lists:', prefix)
        return None

    xar.close()

def merge_metadata(md):
    # given a list md of metadata objects extracted from seaprate nc files from the same platform and cycle,
    # return a single metadata object that sensibly combines the two.
    # assumes consistency check has already been passed

    metadata = {}

    mandatory_unique_keys = ['_id', 'cycle_number', 'basin', 'data_type', 'geolocation', 'instrument', 'timestamp', 'date_updated_argovis', 'fleetmonitoring', 'oceanops'] # yes, 'date_updated_argovis' will be different between the core and synthetic file for a given profile by a few ms, but we intentionally only keep one as this difference isn't meaningful
    for key in mandatory_unique_keys:
        metadata[key] = md[0][key]

    optional_unique_keys = ['profile_direction', 'platform_id', 'doi', 'data_center', 'pi_name', 'country', 'geolocation_argoqc', 'timestamp_argoqc', 'platform_type', 'positioning_system', 'vertical_sampling_scheme', 'wmo_inst_type']
    for key in optional_unique_keys:
        for m in md:
            if key in m:
                metadata[key] = m[key]
                break 

    mandatory_multivalue_keys = ['source_info']
    for key in mandatory_multivalue_keys:
        metadata[key] = []
        for m in md:
            metadata[key].extend(m[key])

    optional_multivalue_keys = ['data_warning']
    for key in optional_multivalue_keys:
        for m in md:
            if key in m:
                if key not in metadata:
                    metadata[key] = []
                metadata[key].extend(m[key])

    if 'data_warning' in metadata:
        metadata['data_warning'] = list(set(metadata['data_warning']))

    return metadata

def merge_data(data_list):
    # given a list of data objects each as returned by extract_data,
    # return a single data object merged into a single pressure axis.
    # all levels from all input objects should be present, with None for data not reported on that level.

    # determine complete set of measurement keys
    data_keys = set([])
    for d in data_list:
        data_keys.update(d['data_keys'])
    data_keys = list(data_keys)
    data_keys.sort()

    # merge data
    data = {}
    degenerate_levels = False
    argo_deep = False
    for d in data_list:
        # handle annotations on first pass
        if "degenerate_levels" in d["data_annotation"] and d["data_annotation"]["degenerate_levels"]:
            degenerate_levels = True
            continue # don't add this data if levels have been duplicated
        keys = d['data_keys']
        for level in d['data']:
            p = level[keys.index('pres')]
            if p not in data: # ie data is numerically keyed by pressure at this point
                data[p] = [None]*len(data_keys)
            for k in keys:
                data[p][data_keys.index(k)] = level[keys.index(k)]
    d = [data[k] for k in sorted(data.keys())] # list of level objects

    # merge data modes
    ## the only possible overlap here should be 'pres'; if there are different data modes between the core and bgc file, use the lowest confidence: R beats A beats D
    data_keys_mode = {}
    pres_mode = []
    for dl in data_list:
        data_keys_mode = {**data_keys_mode, **dl['data_keys_mode']}
        pres_mode.extend(dl['data_keys_mode']['pres'])
    if 'R' in pres_mode:
        data_keys_mode['pres'] = 'R'
    elif 'A' in pres_mode:
        data_keys_mode['pres'] = 'A'
    elif 'D' in pres_mode:
        data_keys_mode['pres'] = 'D'
    else:
        print('error: no sensible data mode found for pres')
    
    return {"data_keys": data_keys, "data_keys_mode": data_keys_mode, "data": [ [cleanup(meas) for meas in level] for level in d], "data_annotation": {"degenerate_levels": degenerate_levels}}

def cleanup(meas):
    # given a measurement, return the measurement after some generic cleanup

    if meas is None:
        return meas

    # qc codes come in as bytes, should be ints
    try:
        ducktype = meas.decode('UTF-8')
        return int(meas)
    except:
        pass

    # use None as missing fill
    if math.isnan(meas):
        return None        

    return round(meas,6) # at most 6 significant decimal places

def parse_location(longitude, latitude):
    # given the raw longitude, latitude from a netcdf file,
    # normalize, clean and log problems

    # official fill value from https://archimer.ifremer.fr/doc/00187/29825/86414.pdf, followed by things seen in the wild
    latitude_fills = [99999, -99.999, -999.0]
    longitude_fills = [99999, -999.999, -999.0] 

    if math.isnan(latitude) or latitude in latitude_fills or math.isnan(longitude) or longitude in longitude_fills:
        print(f'warning: LONGITUDE={longitude}, LATITUDE={latitude}, setting to 0,-90')
        return 0, -90
    elif longitude < -180:
        print('warning: mutating longitude < -180')
        return longitude + 360, latitude
    elif longitude > 180:
        print('warning: mutating longitude > 180')
        return longitude - 360, latitude
    else:
        return longitude, latitude

