from elasticsearch import Elasticsearch
from module.schema.templates import Templates
from module.file.obo import Ontology

def main(arg):
	print("Initializing ElasticSearch cluster")
	print(arg)

def genestation():
	pass
class Genestation:
	def __init__(self, hosts, genomeMetaIndex="genome", **kwargs):
		self.es = ElasticSearch(hosts, **kwargs)
		self.genomeMetaIndex = genomeMetaIndex
	def initializeCluster(self):
		self.loadTemplates()
	def loadTemplates(self):
		for key, value in Templates.items():
			self.es.put_template(id=key, body=value)
	def loadGenome(self, genome):
		pass
	def parseOntology(self, genome):
		pass
