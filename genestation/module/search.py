import sys
import json
from elasticsearch.helpers import scan

def main(arg, es):
	if not es.indices.exists(index=arg.index):
		print("Index does not exist: {0}".format(arg.index), file=sys.stderr)
		return
	count = es.count(index=arg.index, doc_type=arg.doc_type, q=arg.query)
	print('{0} results'.format(count['count']), file=sys.stderr)
	itr = scan(es, index=arg.index, doc_type=arg.doc_type, query={
		'_source': True if arg.source_all else arg.source if arg.source else False,
		'query': { 'query_string': { 'query': arg.query }} if arg.query else { 'match_all': {}},
	})
	for hit in itr:
		if '_source' in hit:
			print('{0}\t{1}\t{2}'.format(hit['_index'], hit['_id'], json.dumps(hit['_source'])))
		else:
			print('{0}\t{1}'.format(hit['_index'], hit['_id']))
