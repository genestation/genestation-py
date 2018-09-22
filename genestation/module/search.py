import sys
import json
from elasticsearch.helpers import scan

def main(arg, es):
	if not es.indices.exists(index=arg.index):
		print("Index does not exist: {0}".format(arg.index), file=sys.stderr)
		return
	count = es.count(index=arg.index, q=arg.query)
	print('{0} results'.format(count['count']), file=sys.stderr)
	itr = scan(es, index=arg.index, query={
		'_source': False,
		'query': { 'query_string': { 'query': arg.query }},
	})
	for hit in itr:
		print('{0}\t{1}'.format(hit['_index'], hit['_id']))
