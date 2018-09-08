import sys
import json

def main(arg, es):
	if not es.indices.exists(index=arg.index):
		print("Index does not exist: {0}".format(arg.index), file=sys.stderr)
		return
	if not es.exists(index=arg.index, doc_type="doc", id=arg.id):
		print("Cannot find document in index {0}: {1}".format(arg.index, arg.id), file=sys.stderr)
		return
	print(json.dumps(es.get(index=arg.index, doc_type="doc", id=arg.id)['_source']))
