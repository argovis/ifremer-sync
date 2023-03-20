from pymongo import MongoClient
from geopy import distance
from itertools import compress
import numpy.ma as ma
import wget, xarray, time, re, datetime, math, os, glob
import util.helpers as h

client = MongoClient('mongodb://database/argo')
db = client.argo

while True:
	logmessage = ''
	lenlog = 0

	# get a random profile, or pick one by ID
	p = list(db.argo.aggregate([{"$sample": {"size": 1}}]))[0]
	#p = list(db.argo.find({"_id":"4900549_182"}))[0]
	m = list(db.argoMeta.find({"_id":p['metadata'][0]}))[0]
	logmessage += 'Checking profile id ' + str(p['_id']) + '\n'

	# transform argovis profile data into dictionary of masked arrays with no elements masked.
	if len(p['data']) == len(p['data_info'][0]):
		p_lookup = {var: ma.masked_array(p['data'][i], [False]*len(p['data'][i])) for i, var in enumerate(p['data_info'][0])}
	else:
		# some cases where data array is empty, ie if everything was a nan upstream
		p_lookup = {var: ma.masked_array([],[]) for i, var in enumerate(p['data_info'][0])}
	nc = []

	# open all upstream netcdf files asociated with the profile; give up on read errors.
	fileOpenFail = False
	for source in p['source']:
		try:
			filename = wget.download(source['url'])
			nc.append({
				"source": source['url'],
				"filename": filename,
				"data": xarray.open_dataset(filename)
			})
		except:
			logmessage += 'failed to download and open ' + source['url'] + '\n'
			fileOpenFail = True
	if fileOpenFail:
		print(logmessage)
		continue

	# make sure nothing has been updated in the last 48h, otherwise move on
	toosoon = False
	for xar in nc:
		# bail out if file was updated in the last 48 h
		ifremer_update = datetime.datetime.strptime(xar['data']['DATE_UPDATE'].to_dict()['data'].decode('UTF-8'),'%Y%m%d%H%M%S')
		if datetime.datetime.now() - datetime.timedelta(hours=48) <= ifremer_update:
			logmessage += 'profile updated at ifremer in the last 48h, skipping validation of ' + str(xar['source']) + ' and related files.\n'
			toosoon = True
	if toosoon:
		print(logmessage)
		continue

	# check data integrity mongo <--> ifremer
	for xar in nc:
		logmessage += 'checking ' + str(xar['source']) + '\n'
		lenlog = len(logmessage)

		LONGITUDE, LATITUDE = h.parse_location(xar['data']['LONGITUDE'].to_dict()['data'][0], xar['data']['LATITUDE'].to_dict()['data'][0], True)

		# metadata validation
		if m['platform'] != xar['data']['PLATFORM_NUMBER'].to_dict()['data'][0].decode('UTF-8').strip():
			logmessage += 'platform_id mismatch at ' + str(xar['source']) + '\n'

		if p['cycle_number'] != int(xar['data']['CYCLE_NUMBER'].to_dict()['data'][0]):
			logmessage += 'cycle_number mismatch at ' + str(xar['source']) + '\n'

		if 'DIRECTION' in list(xar['data'].variables):
			if p['profile_direction'] != xar['data']['DIRECTION'].to_dict()['data'][0].decode('UTF-8'):
				logmessage += 'profile_direction mismatch at ' + str(xar['source']) + '\n'

		reconstruct_id = str(m['platform']) + '_' + str(p['cycle_number']).zfill(3)		
		if 'profile_direction' in p and p['profile_direction'] == 'D':
			reconstruct_id += str(p['profile_direction']) 
		if p['_id'] != reconstruct_id:
			logmessage += 'profile _id mangled for ' + str(xar['source']) + ', ' + str(p['_id']) + ', ' + reconstruct_id + '\n'

		if ('data_warning' not in p) or ('missing_location' not in p['data_warning']):
			if p['basin'] != h.find_basin(LONGITUDE, LATITUDE, True):
				logmessage += 'basin mismatch at ' + str(xar['source']) + '\n'

		if m['data_type'] != 'oceanicProfile':
			logmessage += 'data_type mismatch at ' + str(xar['source']) + '\n'

		if math.isnan(LONGITUDE) or math.isnan(LATITUDE):
			gl = {'type': 'Point', 'coordinates': [0, -90]}
		else:
			gl = {'type': 'Point', 'coordinates': [LONGITUDE, LATITUDE]}
		if p['geolocation'] != gl:
			logmessage += 'geolocation mismatch at ' + str(xar['source']) + '\n'

		if m['instrument'] != 'profiling_float':
			logmessage += 'instrument mismatch at ' + str(xar['source']) + '\n'

		si = {}
		REprefix = re.compile('^[A-Z]*')  
		prefix = REprefix.search(xar['source'].split('/')[-1]).group(0)
		if prefix in ['R', 'D']:
			si['source'] = ['argo_core']
		elif prefix in ['SR', 'SD']:
			si['source'] = ['argo_bgc']
		si['url'] = xar['source']
		si['date_updated'] = datetime.datetime.strptime(xar['data']['DATE_UPDATE'].to_dict()['data'].decode('UTF-8'),'%Y%m%d%H%M%S')
		# note actual checking of si is deferred to the end, after we've assessed whether this is argo_deep

		if m['data_center'] != xar['data']['DATA_CENTRE'].to_dict()['data'][0].decode('UTF-8'):
			logmessage += 'data_center mismatch at ' + str(xar['source']) + '\n'

		xts = xar['data']['JULD'].to_dict()['data'][0]
		if xts is not None:
			td = p['timestamp'] - xts
			if not datetime.timedelta(milliseconds=-1) <= td <= datetime.timedelta(milliseconds=1):
				logmessage += 'timestamp mismatch at ' + str(xar['source']) + '\n'
		elif 'data_warning' not in p or 'missing_timestamp' not in p['data_warning']:
			logmessage += 'failed to warn of missing timestamp at ' + str(xar['source']) + '\n'

		if 'date_updated_argovis' not in p:
			logmessage += 'date_updated_argovis absent from profile derived from ' + str(xar['source']) + '\n'

		if 'PI_NAME' in list(xar['data'].variables):
			if m['pi_name'] != xar['data']['PI_NAME'].to_dict()['data'][0].decode('UTF-8').strip().split(','):
				logmessage += 'pi_name mismatch at ' + str(xar['source']) + '\n'

		if 'POSITION_QC' in list(xar['data'].variables):
			pqc = xar['data']['POSITION_QC'].to_dict()['data'][0]
			if type(pqc) is bytes:
				if p['geolocation_argoqc'] != int(pqc.decode('UTF-8')):
					logmessage += 'geolocation_argoqc mismatch at ' + str(xar['source']) + '\n'
			elif p['geolocation_argoqc'] != -1:
				logmessage += 'geolocation_argoqc mismatch at ' + str(xar['source']) + '\n'

		if 'JULD_QC' in list(xar['data'].variables):
			jqc = xar['data']['JULD_QC'].to_dict()['data'][0]
			if type(jqc) is bytes:
				if p['timestamp_argoqc'] != int(jqc.decode('UTF-8')):
					logmessage += 'timestamp_argoqc mismatch at ' + str(xar['source']) + '\n'
			elif p['timestamp_argoqc'] != -1:
				logmessage += 'timestamp_argoqc mismatch at ' + str(xar['source']) + '\n'

		if m['fleetmonitoring'] != 'https://fleetmonitoring.euro-argo.eu/float/' + str(m['platform']):
			logmessage += 'fleetmonitoring mismatch at ' + str(xar['source']) + '\n'

		if m['oceanops'] != 'https://www.ocean-ops.org/board/wa/Platform?ref=' + str(m['platform']):
			logmessage += 'oceanops mismatch at ' + str(xar['source']) + '\n'

		if 'PLATFORM_TYPE' in list(xar['data'].variables):
			if m['platform_type'] != xar['data']['PLATFORM_TYPE'].to_dict()['data'][0].decode('UTF-8').strip():
				logmessage += 'platform_type mismatch at ' + str(xar['source']) + '\n'

		if 'POSITIONING_SYSTEM' in list(xar['data'].variables):
			if m['positioning_system'] != xar['data']['POSITIONING_SYSTEM'].to_dict()['data'][0].decode('UTF-8').strip():
				logmessage += 'positioning_system mismatch at ' + str(xar['source']) + '\n'

		if 'VERTICAL_SAMPLING_SCHEME' in list(xar['data'].variables):
			if p['vertical_sampling_scheme'] != xar['data']['VERTICAL_SAMPLING_SCHEME'].to_dict()['data'][0].decode('UTF-8').strip():
				logmessage += 'vertical_sampling_scheme mismatch at ' + str(xar['source']) + '\n'

		if 'WMO_INST_TYPE' in list(xar['data'].variables):
			if m['wmo_inst_type'] != xar['data']['WMO_INST_TYPE'].to_dict()['data'][0].decode('UTF-8').strip():
				logmessage += 'wmo_inst_type mismatch at ' + str(xar['source']) + '\n'

		# data validation - dont bother at this time if degenerate levels detected, buyer should very beware on those profiles
		if ('data_warning' not in p) or ("degenerate_levels" not in p['data_warning']):
			allowed_core = ['PRES', 'TEMP', 'PSAL'] # will only consider these variables in core files, anything else should be ignored

			if prefix in ['R', 'D']:
				# check core data
				DATA_MODE = xar['data']['DATA_MODE'].to_dict()['data'][0].decode('UTF-8')
				if DATA_MODE in ['A', 'D']:
					# check adjusted data
					data_sought = [f(x) for x in xar['data']['STATION_PARAMETERS'].to_dict()['data'][0] if x.decode('UTF-8').strip() in allowed_core for f in (lambda name: name.decode('UTF-8').strip()+'_ADJUSTED',lambda name: name.decode('UTF-8').strip()+'_ADJUSTED_QC')]
					nc_pressure = xar['data']['PRES_ADJUSTED'].to_dict()['data'][0]
					nc_pressure_label = 'PRES_ADJUSTED'
				elif DATA_MODE == 'R':
					data_sought = [f(x) for x in xar['data']['STATION_PARAMETERS'].to_dict()['data'][0] if x.decode('UTF-8').strip() in allowed_core for f in (lambda name: name.decode('UTF-8').strip(),lambda name: name.decode('UTF-8').strip()+'_QC')]
					nc_pressure = xar['data']['PRES'].to_dict()['data'][0]
					nc_pressure_label = 'PRES'
				else:
					logmessage += 'unrecognized DATA_MODE for ' + str(xar['source']) + '\n'
				
				nc_data = {h.argo_keymapping(nc_key): [h.cleanup(x) for x in xar['data'][nc_key].to_dict()['data'][0]] for nc_key in data_sought} # upstream data, packed and cleaned like an argovis data key
				for key, nc_vals in nc_data.items():
					for i in range(len(nc_vals)):
						pressure = h.cleanup(nc_pressure[i])
						if pressure is None:
							continue
						elif pressure in p_lookup['pressure'].data.tolist():
							pindex = p_lookup['pressure'].data.tolist().index(pressure)
							if nc_vals[i] != p_lookup[key][pindex]:
								logmessage += f'data mismatch at {key} and pressure {pressure} in {xar["source"]} \n'
							else:
								p_lookup[key].mask[pindex] = True
						else:
							logmessage += f'pressure {pressure} not found in argovis profile from sourcefile {xar["source"]}\n'

			elif prefix in ['SD', 'SR']:
				# check bgc / synth data
				PARAMETER_DATA_MODE = [x.decode('UTF-8') for x in xar['data']['PARAMETER_DATA_MODE'].to_dict()['data'][0]]
				STATION_PARAMETERS = [x.decode('UTF-8').strip() for x in xar['data']['STATION_PARAMETERS'].to_dict()['data'][0]]
				data_sought = []
				for var in zip(PARAMETER_DATA_MODE, STATION_PARAMETERS):
					if var[0] in ['D', 'A']:
						# use adjusted data
						data_sought.extend([var[1]+'_ADJUSTED', var[1]+'_ADJUSTED_QC'])
					elif var[0] == 'R':
						# use unadjusted data
						data_sought.extend([var[1],var[1]+'_QC'])
					else:
						logmessage += 'error: unexpected data mode detected for ' + str(var[1]) + '\n'
				if 'PRES_ADJUSTED' in data_sought:
					nc_pressure = xar['data']['PRES_ADJUSTED'].to_dict()['data'][0]
					nc_pressure_label = 'PRES_ADJUSTED'
				elif 'PRES' in data_sought:
					nc_pressure = xar['data']['PRES'].to_dict()['data'][0]
					nc_pressure_label = 'PRES'
				else:
					logmessage += 'no pressure variable found\n'

				nc_data = {h.argo_keymapping(nc_key).replace('temperature', 'temperature_sfile').replace('salinity', 'salinity_sfile'): [h.cleanup(x) for x in xar['data'][nc_key].to_dict()['data'][0]] for nc_key in data_sought} # upstream data, packed and cleaned like an argovis data key
				for key, nc_vals in nc_data.items():
					for i in range(len(nc_vals)):
						pressure = h.cleanup(nc_pressure[i])
						if pressure is None:
							continue
						elif pressure in p_lookup['pressure'].data.tolist():
							pindex = p_lookup['pressure'].data.tolist().index(pressure)
							if nc_vals[i] != p_lookup[key][pindex]:
								logmessage += f'data mismatch at {key} and pressure {pressure} in {xar["source"]} \n'
							else:
								p_lookup[key].mask[pindex] = True
						else:
							logmessage += f'pressure {pressure} not found in argovis profile from sourcefile {xar["source"]}\n'
			else:
				logmessage += f'unexpected prefix {prefix} found\n'

			if max(nc_pressure) > 2500:
				si['source'].append('argo_deep')

			if si not in p['source']:
				logmessage += 'source mismatch at ' + str(xar['source']) + '\n'
				logmessage += 'mongo source: ' + str(p['source']) + '\n'
				logmessage += '.nc source: ' + str(si) + '\n'
		else:
			logmessage += 'warning: degenerate_levels detected, data array not rechecked\n'

	# if the argovis profile matches the netcdf exactly, then p_lookup should have nothing but masked values and Nones left:
	if ('data_warning' not in p) or ("degenerate_levels" not in p['data_warning']):
		for var in p_lookup:
			if not p_lookup[var].mask.all():
				leftovers = p_lookup[var][p_lookup[var].mask == False]
				if not all(v is None for v in leftovers):
					logmessage += f'unmasked, non-None value in {var}: {p_lookup[var]} at profile {p["_id"]}\n'


	if len(logmessage) != lenlog:
		print(logmessage)
	else:
		f = open('/tmp/roundtrip', 'a')
		f.write(logmessage)
		f.close()

	for f in glob.glob("*.nc"):
		os.remove(f)

	time.sleep(60)
