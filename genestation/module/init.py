from elasticsearch import Elasticsearch
from module.schema.genome_index import GenomeIndex
from module.schema.templates import Templates

def main(arg):
	print("Initializing ElasticSearch cluster")
	es = Elasticsearch(hosts=arg.host)
	# Test connection
	if es.ping() is False:
		print("Cannot access ElasticSearch at", arg.host)
		return
	# Create genome index
	if es.indices.exists(index="genome"):
		print("Genome Index already exists, skipping")
	else:
		print("Creating Genome Index")
		es.indices.create(index="genome", body=GenomeIndex)
	# Create feature index template
	# Create search templates
	#for key, value in Templates.items():
		#self.es.put_template(id=key, body=value)
