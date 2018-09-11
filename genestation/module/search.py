import sys
import json

def main(arg, es):
	if not es.indices.exists(index=arg.index):
		print("Index does not exist: {0}".format(arg.index), file=sys.stderr)
		return
	print(json.dumps(es.search(index=arg.index, q=arg.query)))
