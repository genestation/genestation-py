import os.path
from pysam import VariantFile
import math
from elasticsearch.helpers import bulk

# Python <3.5 workaround
def merge_two_dicts(x,y):
	z = x.copy()
	z.update(y)
	return z


def load_vcf(es, genome, filename, descriptor_in):
	dirname = os.path.dirname(filename)
	good = True
	if not isinstance(descriptor_in, list):
		descriptor_list = [descriptor_in]
	else:
		descriptor_list = descriptor_in
	for descriptor in descriptor_list:
		vcf = {
			"chrom_alias": {},
			"info_key_alias": {},
		}
		if isinstance(descriptor, str):
			vcf["file"] = [descriptor]
		elif isinstance(descriptor, dict):
			#vcf = {**vcf, **descriptor}
			vcf = merge_two_dicts(vcf, descriptor)
		else:
			print('{0}: "vcf" field is malformed'.format(filename), file=sys.stderr)
			good = False
		if isinstance(vcf["file"], str):
			vcf["file"] = [vcf["file"]]
		if not good:
			return
		for vcf_name in vcf["file"]:
			vcf_path = os.path.join(dirname,vcf_name)
			try:
				print("Reading {0}".format(vcf_path))
				print('Loading features', flush=True)
				response = bulk(es, read_vcf(genome, vcf, vcf_path))
				print(response)
			except IOError:
				print('{0}: cannot find GFF "{1}"'.format(filename,vcf_path), file=sys.stderr)

def read_vcf(genome, vcf, vcf_path):
	index_format = 'feature.{0}'.format(genome) + '.{0}'
	info_key_alias = vcf["info_key_alias"]
	vcf_reader = VariantFile(vcf_path)
	for record in vcf_reader.fetch():
		# create core document
		record_tuples = {
			info_key_alias[key] if key in info_key_alias else key: val if len(val) > len(record.alts) else (None,)+val
			for (key, val) in record.info.items()
			if isinstance(val,tuple)
		}
		doc = {
			'genome': genome,
			'ftype': 'sequence_alteration',
			'name': record.id,
			'region': record.contig,
			'start': record.pos - 1,
			'end': record.pos - 1 + len(record.ref),
			'_locrange': {
				'gte': record.pos - 1,
				'lt': record.pos - 1 + len(record.ref),
			},
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
				info_key_alias[key] if key in info_key_alias else key: val if type(val) is not float or not math.isnan(val) else None
				for (key, val) in record.info.items()
				if not isinstance(val,tuple)
			},
		}
		yield {
			'_index': index_format.format(doc['ftype']),
			'_type': 'doc',
			'_id': doc['name'],
			'_source': doc,
		}
