import networkx
import obonet

class Ontology:
	def __init__(self, handle):
		self.graph = obonet.read_obo(handle)
	def __len__(self):
		return len(self.graph)
	def find(self, id_):
		return self.graph.nodes[id_]
	def parents(self, id_):
		return networkx.descendants(self.graph, id_)
	def children(self, id_):
		return networkx.ancestors(self.graph, id_)
