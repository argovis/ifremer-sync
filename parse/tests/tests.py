import util.helpers as h

def pickprof_test():
	assert h.pickprof('D5903649_077.nc') == '077', 'Failed to extract basic profile number'
	assert h.pickprof('D5903649_077D.nc') == '077D', 'Failed to extract decending identifier'

def choose_prefix_test():
	assert h.choose_prefix(['SD', 'SR', 'BD', 'D']) == ['SD', 'D'], 'Failed to choose delayed option'
	assert h.choose_prefix(['SR', 'BD', 'D']) == ['SR', 'D'], 'Failed to choose synth realtime option'
	assert h.choose_prefix(['BD', 'D', 'BR', 'R']) == ['D'], 'Failed to choose delayed option / discard raw BGC'