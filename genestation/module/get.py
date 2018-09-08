import sys
import json
from elasticsearch import Elasticsearch

def main(arg):
	# ElasticSearch
	es = Elasticsearch(arg.host,timeout=600000)
	# Test connection
	if es.ping() is False:
		print("Cannot access ElasticSearch at", arg.host)

	if not es.indices.exists(index=arg.index):
		print("Index does not exist: {0}".format(arg.index), file=sys.stderr)
		return
	if not es.exists(index=arg.index, doc_type="doc", id=arg.id):
		print("Cannot find document in index {0}: {1}".format(arg.index, arg.id), file=sys.stderr)
		return
	print(json.dumps(es.get(index=arg.index, doc_type="doc", id=arg.id)['_source']))
