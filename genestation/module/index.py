import sys
import json
from module.helper.d3 import linear_nice


def main(arg, es):
	if arg.subcommand == "show":
		show(es, arg.index)
	elif arg.subcommand == "make-meta":
		make_meta(es, arg.index)
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

def make_meta(es, index):
	if not es.indices.exists(index=index):
		print("Index does not exist: {0}".format(index), file=sys.stderr)
		return
	meta_index = "meta.{0}".format(index)
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
					field = path[1:]
					if ptr['type'] in numeric_types:
						numeric.append([field, ptr['type']])
					else:
						es.index(index=meta_index, doc_type="doc", id=path, body={
							'field': field,
							'type': ptr['type'],
						})
	# Calculate stats
	for field, field_type in numeric:
		print("Calculating stats for field '{0}'".format(field))
		resp = es.search_template(index=index, body={
			'id': 'meta.field_stats',
			'params': {'field': field},
		})
		stats = resp['aggregations']['field_stats'];
		stats['percentiles'] = resp['aggregations']['field_percentiles']['values'];
		if stats['min'] is not None and stats['max'] is not None and stats['min'] != stats['max']:
			resp = es.search_template(index=index, body={
				'id': 'meta.field_buckets',
				'params': {
					'field': field,
					'ranges': makeHistogramBuckets(stats['min'], stats['max'], 100)
				},
			})
			buckets = resp['aggregations']['field_buckets']['buckets']
			buckets[0]['from'] = stats['min']
			buckets[len(buckets)-1]['to'] = stats['max']
			stats['histogram'] = buckets
		meta = {}
		meta['field'] = field
		meta['type'] = field_type
		meta['stats'] = stats
		es.index(index=meta_index, doc_type="doc", id=field, body=meta)

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
