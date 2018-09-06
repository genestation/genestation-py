#!/usr/bin/env python3

import sys
import argparse
import json
from elasticsearch import Elasticsearch, RequestError
from elasticsearch.helpers import bulk

# Arguments

parser = argparse.ArgumentParser(description='Generate json representation of GFF')
parser.add_argument('index')
parser.add_argument('gff', type=argparse.FileType('r'), default=sys.stdin)
parser.add_argument('--chromosome-aliases', type=argparse.FileType('r'), default=sys.stdin)
parser.add_argument('--enhancer-associations', type=argparse.FileType('r'), default=sys.stdin)
parser.add_argument('--host', default='localhost:9200')
parser.add_argument('--species', default='Homo sapiens')

arg = parser.parse_args()

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

print('Reading Chromosome aliases', flush=True)
chromosome_alias = {}
for line in arg.chromosome_aliases:
	line = line.rstrip().split('\t')
	if len(line) < 2:
		continue #TODO fail
	chromosome_alias[line[0]] = line[1]

print('Reading Enhancer associations', flush=True)
enhancer_associations = {}
next(arg.enhancer_associations)
for line in arg.enhancer_associations:
	line = line.rstrip().split('\t')
	if len(line) < 6:
		continue #TODO fail
	enhancer = line[0]
	start = line[1]
	end = line[2]
	gene = line[3]
	rsqr = line[4]
	fdr = line[5]
	if gene not in enhancer_associations:
		enhancer_associations[gene] = []
	enhancer_associations[gene].append({
		'name': enhancer,
		'ftype': 'enhancer',
		'start': int(start),
		'end': int(end),
		'_locrange': {
			'gte': int(start),
			'lt': int(end),
		},
		'data': {
			'fantom5': {
				'rsqr': rsqr,
				'fdr': fdr,
			}
		}
	})

genes = {}
feature = {}

print('Reading GFF file', flush=True)
lineno = 0
curr = None
currid = None
for line in arg.gff:
	lineno += 1

	if line.startswith('#'):
		continue
	line = line.rstrip().split('\t')
	if len(line) < 9:
		raise Exception('GFF column error: line {}'.format(lineno))
	attributes = {keyval[0]: keyval[1].split(',')
		for keyval in
		map(lambda itr: itr.split('='), line[COLUMN.ATTRIBUTES].split(';'))}

	ftype = line[COLUMN.TYPE]
	doc = {
		'ftype': ftype,
		'srcfeature': chromosome_alias[line[COLUMN.SEQID]] if line[COLUMN.SEQID] in chromosome_alias else line[COLUMN.SEQID],
		'start': int(line[COLUMN.START]) - 1,
		'end': int(line[COLUMN.END]),
		'_locrange': {
			'gte': int(line[COLUMN.START]) - 1,
			'lt': int(line[COLUMN.END]),
		}
	}

	strand = line[COLUMN.STRAND]
	if strand == '+':
		doc['strand'] = 1
	elif strand == '-':
		doc['strand'] = -1

	gffid = attributes['ID'][0]
	if 'Name' in attributes:
		doc['name'] = attributes['Name'][0]
	if 'Dbxref' in attributes:
		doc['dbxref'] = attributes['Dbxref']

	# Storing doc
	feature[gffid] = doc
	if 'Parent' in attributes:
		parent = attributes['Parent'][0]
		if 'child' not in feature[parent]:
			feature[parent]['child'] = []
		feature[parent]['child'].append(doc)
	if ftype == 'gene':
		doc['association'] = []
		if doc['name'] in enhancer_associations:
			for enhancer in enhancer_associations[doc['name']]:
				doc['association'].append(enhancer)
		genes[doc['name']] = doc

#print(json.dumps(genes['FSHR'], indent='\t'))
#raise Exception

data_properties={}
mapping = {
	"mappings" : {
		"_default_" : {
			"dynamic" : False,
			"properties" : {
				"name" : { "type" : "keyword" },
				"dbxref" : { "type" : "keyword" },
				"ftype" : { "type" : "keyword" },
				"srcfeature" : { "type" : "keyword" },
				"start" : { "type" : "integer" },
				"end" : { "type" : "integer" },
				"_locrange" : { "type" : "integer_range" },
				"strand" : { "type" : "integer" },
				"phase" : { "type" : "integer" },
				"association" : { "dynamic" : True },
				"data": {
					"dynamic" : True,
					"properties": data_properties
				},
	}}}
}
# ElasticSearch
es = Elasticsearch(arg.host,timeout=600000)
doc_type = arg.species.replace(' ','_')
try:
  es.indices.create(index=arg.index, body=mapping)
except RequestError:
  print('Skipping index creation')
sys.stdout.flush()

count = len(genes)
cycle = 128
curr = 0
bulk_data = []
def process_buffer(eof=False):
  global curr
  global bulk_data
  if not eof:
    curr += 1
  if curr%cycle == 0 or eof:
    response = bulk(es,
      index=arg.index,
      actions=bulk_data,
    )
    print(curr/count,response)
    sys.stdout.flush()
    bulk_data = []
for name, doc in genes.items():
  # Add doc to buffer
  bulk_data.append({
    '_type': doc_type,
    '_id': name.upper(),
    '_source': doc,
  })
  # (per cycle) Bulk index buffer
  process_buffer()
# loop end, process remaining buffer
process_buffer(True)
