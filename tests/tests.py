import datetime, copy
import util.helpers as h

def test_pickprof():
	assert h.pickprof('D5903649_077.nc') == '077', 'Failed to extract basic profile number'
	assert h.pickprof('D5903649_077D.nc') == '077D', 'Failed to extract decending identifier'

def test_choose_prefix():
	assert h.choose_prefix(['SD', 'SR', 'BD', 'D']) == ['SD', 'D'], 'Failed to choose delayed option'
	assert h.choose_prefix(['SR', 'BD', 'D']) == ['SR', 'D'], 'Failed to choose synth realtime option'
	assert h.choose_prefix(['BD', 'D', 'BR', 'R']) == ['D'], 'Failed to choose delayed option / discard raw BGC'

def test_merge_metadata():
    dt = datetime.datetime(2016,8,1,12,0,0)
    core_meta = {
        '_id': '999900_123', 
        'cycle_number': 123, 
        'basin': 99, 
        'data_type': 'oceanicProfile', 
        'geolocation': {"type": "Point", "coordinates": [1.234, 5.678]}, 
        'instrument': 'profiling_float', 
        'timestamp': 1000000, 
        'date_updated_argovis': dt, 
        'fleetmonitoring': 'https://example.com/', 
        'oceanops': 'https://example.com/', 
        'source': [{'source': 'argo_core', 'url': 'https://example.com', 'date_updated': dt}],
        'pi_name': ['Dr. Float', 'Boaty McBoatface'],
    }

    # nominal no problems case
    synt_meta = copy.deepcopy(core_meta)
    synt_meta['source'][0]['source'] = 'argo_bgc'
    merged_meta = copy.deepcopy(core_meta)
    merged_meta['source'] = core_meta['source'] + synt_meta['source']
    assert h.merge_metadata([core_meta, synt_meta]) == merged_meta, 'Failed to merge metadata with identical records'

    # mandatory unique field mismatch
    synt_meta = copy.deepcopy(core_meta)
    synt_meta['source'][0]['source'] = 'argo_bgc'
    synt_meta['geolocation'] = {"type": "Point", "coordinates": [0, 0]}
    merged_meta = copy.deepcopy(core_meta)
    merged_meta['source'] = core_meta['source'] + synt_meta['source']
    merged_meta['data_warning'] = ['bgc_mismatch']
    merged_meta['bgc_mismatches'] = {'geolocation': {"type": "Point", "coordinates": [0, 0]}}
    assert h.merge_metadata([core_meta, synt_meta]) == merged_meta, 'Failed to merge metadata with different geolocation and bgc_mismatch warning'
    assert h.merge_metadata([synt_meta, core_meta]) == merged_meta, 'Order shouldnt matter in input fragments'

    # optional unique field mismatch
    synt_meta = copy.deepcopy(core_meta)
    synt_meta['source'][0]['source'] = 'argo_bgc'
    synt_meta['pi_name'] = ['FLOAT', 'MCBOATFACE']
    merged_meta = copy.deepcopy(core_meta)
    merged_meta['source'] = core_meta['source'] + synt_meta['source']
    merged_meta['data_warning'] = ['bgc_mismatch']
    merged_meta['bgc_mismatches'] = {'pi_name':['FLOAT', 'MCBOATFACE']}
    assert h.merge_metadata([core_meta, synt_meta]) == merged_meta, 'Failed to merge metadata with different PI names.'