#!/bin/python

import re
from collections import defaultdict
from elasticsearch import Elasticsearch, RequestError
from elasticsearch.helpers import bulk
import json
import math
from pysam import VariantFile

vcf_file='/raid/chado/analyses/1000g/merged/ALL.snps.MERGED.GRCh38.bcf'
#vcf_file='/raid/chado/data/dbSNP/common_all_20170710.vcf.gz'
vcf_column_file='/raid/chado/analyses/1000g/merged/bcf-columns.tsv'
gat_files={
	'phewas':'/raid/chado/data/phewas/phewas-catalog.gat',
	'gwas':'/raid/chado/data/gwas_catalog/gwas-catalog.gat',
}
elasticnode='rokasgate2:9200'
index='variant_v1.4'
doc_type = 'Homo_sapiens'
count = 77538346 # bcftools stats
#dbSNP common_all.count = 37463647

###

print('Index:',index)

es = Elasticsearch(elasticnode,timeout=600000)

data_properties = {
	gat_key: {"type": "nested"}
	for gat_key in gat_files
}
mapping = {
	"mappings" : {
		"_default_" : {
			"properties" : {
				"name" : { "type" : "keyword" },
				"ftype" : { "type" : "keyword" },
				"srcfeature" : { "type" : "keyword" },
				"start" : { "type" : "integer" },
				"end" : { "type" : "integer" },
				"_locrange" : { "type" : "integer_range" },
				"variant": { "properties": {
					"ref": { "properties": {
						"base": { "type": "text" },
					}},
					"alt": {
						"type": "nested",
						"properties": {
							"base": { "type": "text" },
					}},
				}},
				"data": { "properties": data_properties },
	}}}
}

if not es.indices.exists(index=index):
	es.indices.create(index=index, body=mapping)
else:
	print('Skipping index creation', flush=True)

class BulkLoader:
	def __init__(self, es, index, doc_type, count=None, cycle=8192, **kwargs):
		self.es = es
		self.index = index
		self.doc_type = doc_type
		self.kwargs = kwargs
		self.count = count
		self.cycle = cycle
		self.kwargs = kwargs
		self.curr = 0
		self.bulk_data = []
	def add(self, *actions):
		# Add actions to buffer
		self.bulk_data.extend(actions)
		self.curr += len(actions)
		if len(self.bulk_data) >= self.cycle:
			self.process_buffer(True)
	def process_buffer(self, auto=False):
		# filter out existing docs
		init_len = len(self.bulk_data)
		mget_docs = self.es.mget(index=self.index, doc_type=self.doc_type, _source=False,
			body={'ids':[action['_id'] for action in self.bulk_data]})['docs']
		self.bulk_data = [action for idx, action in enumerate(self.bulk_data)
			if not mget_docs[idx]['found']]
		existing = init_len - len(self.bulk_data)
		if len(self.bulk_data) < self.cycle and auto:
			if self.count:
				print(self.curr/self.count, end=' ')
			print((existing, 'existing'), flush=True)
			return
		else:
			# bulk index
			response = bulk(self.es, index=self.index, doc_type=self.doc_type, actions=self.bulk_data, **self.kwargs)
			if self.count:
				print(self.curr/self.count, end=' ')
			if existing > 0:
				print((existing, 'existing'), end=' ')
			print(response, flush=True)
			self.bulk_data = []

print('Reading GAT files', flush=True)
genomic_annotations = {}
for gat_key, filename in gat_files.items():
	with open(filename) as gat_file:
		header = next(gat_file).strip().split('\t')
		for line in gat_file:
			line = line.strip().split('\t')
			features = json.loads(line[0])
			if type(features) is not list:
				features = [features]
			annotations = [json.loads(value) for value in line[1:]]
			for feature in features:
				if feature not in genomic_annotations:
					genomic_annotations[feature] = {}
				if gat_key not in genomic_annotations[feature]:
					genomic_annotations[feature][gat_key] = []
				genomic_annotations[feature][gat_key].append({
					header[idx+1]: val
					for idx, val in enumerate(annotations)
				})

vcf_fields = {}
with open(vcf_column_file) as column_file:
	for line in column_file:
		line = line.strip().split('\t')
		vcf_fields[line[0]] = line[1]

print('Reading VCF file', flush=True)
bulk_loader = BulkLoader(es, index, doc_type, count)
vcf_reader = VariantFile(vcf_file)
for record in vcf_reader.fetch():
	doc_id = record.id
	# create core document
	record_tuples = {
		vcf_fields[key] if key in vcf_fields else key: val if len(val) > len(record.alts) else (None,)+val
		for (key, val) in record.info.items()
		if isinstance(val,tuple)
	}
	doc = {
		'name': record.id,
		'ftype': 'sequence_alteration',
		'srcfeature': record.contig,
		'start': record.pos - 1,
		'end': record.pos - 1 + len(record.ref),
		'_locrange': {
			'gte': record.pos - 1,
			'lt': record.pos - 1 + len(record.ref),
		},
		'strand': 0,
		'phase': None,
		'variant': {
			'ref': {
				'base': record.ref,
				'data': {
					key: val[0] if type(val[0]) is not float or not math.isnan(val[0]) else None
					for (key, val) in record_tuples.items()
					if val[0] is not None
				},
			},
			'alt': [{
				'base': alt,
				'data': {
					key: val[idx+1] if type(val[idx+1]) is not float or not math.isnan(val[idx+1]) else None
					for (key, val) in record_tuples.items()
				},
			} for idx,alt in enumerate(record.alts)],
		},
		'data': {
			vcf_fields[key] if key in vcf_fields else key: val if type(val) is not float or not math.isnan(val) else None
			for (key, val) in record.info.items()
			if not isinstance(val,tuple)
		},
	}
	if doc_id in genomic_annotations:
		doc['data'].update(genomic_annotations[doc_id])

	#DEBUG
	#print(json.dumps(doc, indent='  '))
	#raise Exception

	bulk_loader.add({
		'_id': doc_id,
		'_source': doc,
	})
# loop end, process remaining buffer
bulk_loader.process_buffer()
