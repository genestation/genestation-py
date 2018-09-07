import sys
import json
from module.file.gff import load_gff
from elasticsearch import Elasticsearch

# ElasticSearch
es = Elasticsearch(arg.host,timeout=600000)
# Test connection
if es.ping() is False:
	print("Cannot access ElasticSearch at", arg.host)

def main(arg):
	for handle in arg.descriptor:
		try:
			descriptor = json.load(handle)
			load(handle.name, descriptor)
		except ValueError as e:
			print("{0}: {1}".format(handle.name,e), file=sys.stderr)

def load(filename, descriptor):
	# Validate descriptor
	good = True
	if "genus" not in descriptor:
		print('{0}: Incomplete genome descriptor: missing "genus"'.format(filename), file=sys.stderr)
		good = False
	if "species" not in descriptor:
		print('{0}: Incomplete genome descriptor: missing "species"'.format(filename), file=sys.stderr)
		good = False
	if "version" not in descriptor:
		print('{0}: Incomplete genome descriptor: missing "version"'.format(filename), file=sys.stderr)
		good = False
	if not good:
		return
	organism = [descriptor["genus"],descriptor["species"]]
	if "subspecies" in descriptor:
		organism.append(descriptor["subspecies"])
	genome = "_".join(organism).lower().replace(".","_").replace(" ","_") \
	  + "." + descriptor["version"].lower().replace(".","_").replace(" ","_")
	print("Genome:",genome)
	# Read GFF
	if "gff" in descriptor:
		load_gff(es, genome, filename, descriptor["gff"])

