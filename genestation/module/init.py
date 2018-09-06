from elasticsearch import Elasticsearch
from module.schema.genome_index import GenomeIndex
from module.schema.feature_index import FeatureIndexTemplate, FeatureSearchTemplate

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
	if es.indices.exists_template(name="feature"):
		print("Feature Index Template already exists, skipping")
	else:
		print("Putting Feature Index Template")
		es.indices.put_template(name="feature", body=FeatureIndexTemplate)
	# Create search templates
	print("Putting Feature Index search template scripts")
	for key, value in FeatureSearchTemplate.items():
		print("-",key)
		es.put_script(id=key, body=value, lang="mustache")
