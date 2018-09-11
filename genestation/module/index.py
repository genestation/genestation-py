import sys
import json
from module.helper.d3 import linear_nice


def main(arg, es):
	if arg.subcommand == "show":
		show(es, arg.index)
	elif arg.subcommand == "make-stats":
		make_stats(es, arg.index)
	else:
		list_indexes(es)

def list_indexes(es):
	resp = es.indices.get_alias("*")
	if resp:
		for key in resp:
			print('{0}:\t{1}'.format(key,es.count(index=key)['count']))

def show(es, index):
	if not es.indices.exists(index=index):
		print("Index does not exist: {0}".format(index), file=sys.stderr)
		return
	resp = es.indices.get(index=index)
	print(json.dumps(resp, indent='  '))

def make_stats(es, index):
	if not es.indices.exists(index=index):
		print("Index does not exist: {0}".format(index), file=sys.stderr)
		return
	stats_index = "stats.{0}".format(index)
	resp = es.indices.get_mapping(index=index)
	# Determine numeric fields
	numeric = []
	numeric_types = set(['long','integer','short','byte','double','float','half_float','scaled_float'])
	for name in resp:
		mappings = resp[name]['mappings']
		for doc_type in mappings:
			path_stack, stack = [""], [mappings[doc_type]]
			while stack:
				ptr = stack.pop()
				path = path_stack.pop()
				if 'properties' in ptr:
					properties = ptr['properties']
					for key in properties:
						stack.append(properties[key])
						path_stack.append('.'.join([path,key]))
				elif 'type' in ptr:
					if ptr['type'] in numeric_types:
						numeric.append([path, ptr['type']])
					else:
						es.index(index=stats_index, doc_type="doc", id=path, body={
							'field': path,
							'type': ptr['type'],
						})
	# Calculate stats
	for field, field_type in numeric:
		field = field[1:]
		print("Calculating stats for field '{0}'".format(field))
		resp = es.search_template(index=index, body={
			'id': 'stats.field_stats',
			'params': {'field': field},
		})
		stats = resp['aggregations']['field_stats'];
		stats['percentiles'] = resp['aggregations']['field_percentiles']['values'];
		if stats['min'] is not None and stats['max'] is not None and stats['min'] != stats['max']:
			nice_domain = linear_nice([stats['min'],stats['max']],4)
			stats['nice_min'] = nice_domain[0]
			stats['nice_max'] = nice_domain[1]
			resp = es.search_template(index=index, body={
				'id': 'stats.field_buckets',
				'params': {
					'field': field,
					'ranges': makeHistogramBuckets(stats['nice_min'], stats['nice_max'], 100)
				},
			})
			buckets = resp['aggregations']['field_buckets']['buckets']
			buckets[0]['from'] = stats['nice_min']
			buckets[len(buckets)-1]['to'] = stats['nice_max']
			stats['histogram'] = buckets
		stats['field'] = field
		stats['type'] = field_type
		es.index(index=stats_index, doc_type="doc", id=field, body=stats)

def makeHistogramBuckets(fmin, fmax, numBuckets):
	interval = (fmax - fmin) / numBuckets

	x = fmin + interval
	last = x
	ranges = [{'to': x}]
	i = 1
	while i < numBuckets-1:
		x += interval
		ranges.append({'from': last, 'to': x})
		last = x
		i += 1
	ranges.append({'from': x})
	return ranges
