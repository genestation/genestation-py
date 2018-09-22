#!/usr/bin/env python3

import sys
import os.path
from urllib.parse import unquote as url_unquote
from elasticsearch.helpers import bulk

# Python <3.5 workaround
def merge_two_dicts(x,y):
	z = x.copy()
	z.update(y)
	return z

# GFF
class COLUMN:
	SEQID = 0
	SOURCE = 1
	TYPE = 2
	START = 3
	END = 4
	SCORE = 5
	STRAND = 6
	PHASE = 7
	ATTRIBUTES = 8
#

def load_gff(es, genome, filename, descriptor_in):
	dirname = os.path.dirname(filename)
	good = True
	if not isinstance(descriptor_in, list):
		descriptor_list = [descriptor_in]
	else:
		descriptor_list = descriptor_in
	for descriptor in descriptor_list:
		gff = {
			"ftype": "gene",
			"seqid_alias": {},
			"alias_attr": "Alias",
			"dbxref_attr": "Dbxref",
			"data_attr": {},
		}
		if isinstance(descriptor, str):
			gff["file"] = [descriptor]
		elif isinstance(descriptor, dict):
			#gff = {**gff, **descriptor}
			gff = merge_two_dicts(gff, descriptor)
		else:
			print('{0}: "gff" field is malformed'.format(filename), file=sys.stderr)
			good = False
		if isinstance(gff["file"], str):
			gff["file"] = [gff["file"]]
		if not good:
			return
		for gff_name in gff["file"]:
			gff_path = os.path.join(dirname,gff_name)
			try:
				with open(gff_path) as gff_file:
					print("Reading {0}".format(gff_path))
					features = read_gff(genome, gff, gff_file)
					elastic_gff(es, genome, features)
			except IOError:
				print('{0}: cannot find GFF "{1}"'.format(filename,gff_path), file=sys.stderr)

def read_gff(genome, gff, gff_file):
	insert_ftype = gff["ftype"]
	seqid_alias = gff["seqid_alias"]
	alias_attr = gff["alias_attr"]
	dbxref_attr = gff["dbxref_attr"]
	data_attr = gff["data_attr"]

	feature = {}
	insert_features = []

	print('Reading GFF file', flush=True)
	lineno = 0
	curr = None
	currid = None
	for line in gff_file:
		lineno += 1

		if line.startswith('#'):
			continue
		line = line.rstrip().split('\t')
		if len(line) < 9:
			raise Exception('GFF column error: line {}'.format(lineno))
		attributes = {keyval[0]: list(map(lambda value: url_unquote(value), keyval[1].split(',')))
			for keyval in
			map(lambda itr: itr.split('='), line[COLUMN.ATTRIBUTES].split(';'))}

		ftype = line[COLUMN.TYPE]
		doc = {
			'genome': genome,
			'ftype': ftype,
			'region': seqid_alias[line[COLUMN.SEQID]] if line[COLUMN.SEQID] in seqid_alias else line[COLUMN.SEQID],
			'start': int(line[COLUMN.START]) - 1,
			'end': int(line[COLUMN.END]),
			'locrange': {
				'gte': int(line[COLUMN.START]) - 1,
				'lt': int(line[COLUMN.END]),
			}
		}

		# Location
		loc = {
			'start': int(line[COLUMN.START]) - 1,
			'end': int(line[COLUMN.END]),
		}
		strand = line[COLUMN.STRAND]
		if strand == '+':
			loc['strand'] = 1
		elif strand == '-':
			loc['strand'] = -1
		elif strand == '.':
			loc['strand'] = 0
		else:
			loc['strand'] = None
		phase = line[COLUMN.PHASE]
		if phase in ["0","1","2"]:
			loc['phase'] = int(phase)
		else:
			loc['phase'] = None

		gffid = attributes['ID'][0]
		if 'Name' in attributes:
			doc['name'] = attributes['Name'][0]
		if alias_attr in attributes:
			doc['name'].extend(attributes[alias_attr])
		if dbxref_attr in attributes:
			doc['dbxref'] = attributes[dbxref_attr]

		# Data
		data = {}
		for key in attributes:
			if key in data_attr:
				data[data_attr[key]] = attributes[key]

		# Storing doc
		if gffid in feature:
			#feature[gffid] = {**feature[gffid], **doc}
			feature[gffid] = merge_two_dicts(feature[gffid], doc)
			if loc['start'] < feature[gffid]['start']:
				feature[gffid]['locrange']['gte'] = loc['start']
				feature[gffid]['start'] = loc['start']
			if loc['end'] > feature[gffid]['end']:
				feature[gffid]['locrange']['lt'] = loc['end']
				feature[gffid]['end'] = loc['end']
			feature[gffid]['loc'].append(loc)
			#feature[gffid]['data'] = {**feature[gffid]['data'], **data}
			feature[gffid]['data'] = merge_two_dicts(feature[gffid]['data'], data)
		else:
			doc['locrange'] = {
				'gte': loc['start'],
				'lt': loc['end'],
			}
			doc['start'] = loc['start']
			doc['end'] = loc['end']
			doc['loc'] = [loc]
			doc['data'] = data
			feature[gffid] = doc
			if 'Parent' in attributes:
				parent = attributes['Parent'][0]
				if 'child' not in feature[parent]:
					feature[parent]['child'] = []
				feature[parent]['child'].append(doc)
			if ftype in insert_ftype and 'name' in doc:
				insert_features.append(feature[gffid])

	# Return features to load
	return insert_features

def elastic_gff(es, genome, features):
	print('Loading features', flush=True)
	index_format = 'feature.{0}'.format(genome) + '.{0}'
	def gendata():
		for doc in features:
			yield {
				'_index': index_format.format(doc['ftype']),
				'_type': 'doc',
				'_id': doc['name'],
				'_source': doc,
			}
	response = bulk(es, gendata())
	print(response)
