from module.schema.genome_index import GenomeIndex
from module.schema.feature_index import FeatureIndexTemplate, FeatureSearchTemplate
from module.schema.stats_index import StatsIndexTemplate, StatsSearchTemplate

def main(arg, es):
	print("Initializing ElasticSearch cluster")
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
	# Create feature search templates
	print("Putting Feature Index search template scripts")
	for key, value in FeatureSearchTemplate.items():
		print("-",key)
		es.put_script(id=key, body=value, lang="mustache")
	# Create stats index template
	if es.indices.exists_template(name="stats"):
		print("Stats Index Template already exists, skipping")
	else:
		print("Putting Stats Index Template")
		es.indices.put_template(name="stats", body=StatsIndexTemplate)
	# Create stats search templates
	print("Putting Stats Index search template scripts")
	for key, value in StatsSearchTemplate.items():
		print("-",key)
		es.put_script(id=key, body=value, lang="mustache")
