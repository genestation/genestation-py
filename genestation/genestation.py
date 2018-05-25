from elasticsearch import Elasticsearch
from schema.templates import Templates

def genestation():
	pass
class Genestation:
	def __init__(self, hosts, genomeMetaIndex="genomes", **kwargs):
		self.es = ElasticSearch(hosts, **kwargs)
		self.genomeMetaIndex = genomeMetaIndex
	def initializeCluster(self):
		self.loadTemplates()
	def loadTemplates(self):
		for key, value in Templates.items():
			self.es.put_template(id=key, body=value)
