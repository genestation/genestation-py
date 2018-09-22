import sys
import os.path
import json
from elasticsearch.helpers import bulk

# Python <3.5 workaround
def merge_two_dicts(x,y):
	z = x.copy()
	z.update(y)
	return z

# TODO handle numeric inference problems

def load_tsjv(es, genome, filename, descriptor_in):
	dirname = os.path.dirname(filename)
	good = True
	if not isinstance(descriptor_in, list):
		descriptor_list = [descriptor_in]
	else:
		descriptor_list = descriptor_in
	for descriptor in descriptor_list:
		tsjv = {
			"ftype": "gene",
			"feature_col": "feature",
			"start_col": None,
			"end_col": None,
			"region_col": None,
			"association_cols": "association",
			"association_genome": genome,
			"association_genome_col": None,
			"association_ftype": "gene",
			"association_ftype_col": None,
			"data_cols": None,
			"association_data_cols": None,
		}
		if isinstance(descriptor, str):
			tsjv["file"] = [descriptor]
		elif isinstance(descriptor, dict):
			#vcf = {**vcf, **descriptor}
			tsjv = merge_two_dicts(tsjv, descriptor)
		else:
			print('{0}: "tsjv" field is malformed'.format(filename), file=sys.stderr)
			good = False
		if isinstance(tsjv["file"], str):
			tsjv["file"] = [tsjv["file"]]
		if not good:
			return
		for tsjv_name in tsjv["file"]:
			tsjv_path = os.path.join(dirname,tsjv_name)
			try:
				with open(tsjv_path) as tsjv_file:
					print("Reading {0}".format(tsjv_path), flush=True)
					response = bulk(es, read_tsjv(genome, tsjv, tsjv_file))
					print(response)
			except IOError:
				print('{0}: cannot find TSJV "{1}"'.format(filename,tsjv_path), file=sys.stderr)
			except TsjvException:
				print('{0}: TSJV syntax error "{1}"'.format(filename,tsjv_path), file=sys.stderr)

class TsjvException(Exception):
    pass

def read_tsjv(genome, tsjv, tsjv_file):
	index_format = 'feature.{0}'.format(genome) + '.{0}'
	# Read header
	header_line = next(tsjv_file)
	header = [json.loads(item) for item in header_line.strip().split('\t')]
	if len([item for item in header if not isinstance(item,str)]) > 0:
		raise TsjvException('Malformed TSJV header')
	try:
		feature_idx = header.index(tsjv['feature_col'])
	except ValueError:
		raise TsjvException('feature_col `{0}` TSJV header'.format(tsjv['feature_col']))
	start_idx = header.index(tsjv['start_col']) if tsjv['start_col'] is not None and tsjv['start_col'] in header else None
	end_idx = header.index(tsjv['end_col']) if tsjv['end_col'] is not None and tsjv['end_col'] in header else None
	region_idx = header.index(tsjv['region_col']) if tsjv['region_col'] is not None and tsjv['region_col'] in header else None
	association_genome_idx = header.index(tsjv['association_genome_col']) if tsjv['association_genome_col'] is not None and tsjv['association_genome_col'] in header else None
	association_ftype_idx = header.index(tsjv['association_ftype_col']) if tsjv['association_ftype_col'] is not None and tsjv['association_ftype_col'] in header else None
	data_cols = tsjv['data_cols']
	data_idxs = [header.index(col) for col in data_cols if col in header] if data_cols is not None else None
	association_cols = tsjv['association_cols']
	association_idxs = [header.index(col) for col in association_cols if col in header] if association_cols is not None else None
	association_data_cols = tsjv['association_data_cols']
	association_data_idxs = [header.index(col) for col in association_data_cols if col in header] if association_data_cols is not None else None
	# Read body
	docs = {}
	for line in tsjv_file:
		line = line.strip().split('\t')
		if len(line) != len(header):
			raise TsjvException('Inconsistent TSJV column arity')
		# Base document
		names = json.loads(line[feature_idx])
		if type(names) is not list:
			names = [names]
		for name in names:
			if name not in docs:
				docs[name] = {
					'genome': genome,
					'ftype': tsjv['ftype'],
					'name': name,
				}
			if region_idx is not None:
				docs[name]['region'] = json.loads(line[region_idx])
			if start_idx is not None and end_idx is not None:
				loc = {
					'start': json.loads(line[start_idx]),
					'end': json.loads(line[end_idx]),
				}
				docs[name]['start'] = loc['start']
				docs[name]['end'] = loc['end']
				docs[name]['locrange'] = {
					'gte': loc['start'],
					'lt': loc['end'],
				}
			# Data fields TODO nesting
			if data_idxs is not None:
				if 'data' not in docs:
					docs['data'] = {}
				for idx, data_idx in enumerate(data_idxs):
					colname = data_cols[idx]
					docs[name]['data'][colname] = json.loads(line[data_idx])
					# WITH nesting
					#if colname not in docs[name]['data']:
					#	docs[name]['data'][colname] = []
					#docs[name]['data'][colname].append(json.loads(line[data_idx]))
			# Association fields TODO nesting
			if association_idxs is not None:
				if 'association' not in docs[name]:
					docs[name]['association'] = []
				association_names = [json.loads(line[idx]) for idx in association_idxs]
				#association_names = map(lambda names: names if type(names) is list else [names], [json.loads(line[idx]) for idx in association_idxs])
				#association_names = list(set([name for names in association_names for name in names]))
				association = {
					'genome': json.loads(line[association_genome_idx]) if association_genome_idx is not None else tsjv['association_genome'],
					'ftype': json.loads(line[association_ftype_idx]) if association_ftype_idx is not None else tsjv['association_ftype'],
					'name': association_names
				}
				association_data = {}
				for idx, association_data_idx in enumerate(association_data_idxs):
					colname = association_data_cols[idx]
					association_data[colname] = json.loads(line[association_data_idx])
				association['data'] = association_data
				docs[name]['association'].append(association)
	print('Loading features', flush=True)
	for name, doc in docs.items():
		script = ''
		params = {}
		if 'data' in doc:
			for key in doc['data']:
				datakey = 'data.{0}'.format(key)
				script += 'ctx._source.data.{0} = params.{1};'.format(key,datakey)
				params[datakey] = json.dumps(doc['data'][key])
		if 'association' in doc:
			script += 'ctx._source.association.add(params.association);'
			params['association'] = json.dumps(doc['association'])
		yield {
			'_op_type': 'update',
			'_index': index_format.format(doc['ftype']),
			'_type': 'doc',
			'_id': doc['name'],
			'script': {
				'source': script,
				'lang': 'painless',
			},
			'upsert': doc,
		}
