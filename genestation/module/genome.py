import sys
from elasticsearch.helpers import scan

def main(arg, es):
	if arg.subcommand == "show":
		show(es, arg.genome)
	else:
		list_genomes(es)

def list_genomes(es):
	results = scan(es, index='genome', query={
		'query': {'match_all': {}}
	})
	for result in results:
		print(result['_id'])

def show(es, genome):
	if not es.exists(index='genome', doc_type="doc", id=genome):
		print("Cannot find genome: {0}".format(genome), file=sys.stderr)
		return
	resp = es.get(index='genome', doc_type="doc", id=genome)
	print('* genome {0}'.format(resp['_id']))
	source = resp['_source']
	print('  Organism: {0}'.format(" ".join([source["genus"],source["species"],source["subspecies"] if source["subspecies"] else ''])))
	print('  Version: {0}'.format(source["version"]))
	feature_prefix = 'feature.{0}.'.format(genome)
	resp = es.indices.get_alias(index='{0}*'.format(feature_prefix))
	if resp:
		print('  Features:')
		for key in resp:
			print('    {0}:\t{1}'.format(key[len(feature_prefix):],es.count(index=key)['count']))
