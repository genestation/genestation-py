#!/usr/bin/env python3

import argparse
from elasticsearch import Elasticsearch
from schema.templates import Templates
from module.ontology import Ontology

PROG='genestation'
VERSION="0.0.1"

# Argparse
parser = argparse.ArgumentParser(prog=PROG)
parser.add_argument('--version', action='version',
	version='%(prog)s '+VERSION)
parser.add_argument('--host', action='append',
	help='url(s) to the elasticsearch node; default localhost:9200')
subparsers = parser.add_subparsers(
	title='subcommands')

# init command parser
parser_init = subparsers.add_parser('init', description='initialize a new genestation instance')

# load command parser
parser_load = subparsers.add_parser('load', description='load genomic data into a genestation instance')
parser_load.add_argument('descriptor', nargs='+', type=argparse.FileType('r'),
	help='Genomic Data Descriptor JSON')

# Parse arguments
arg = parser.parse_args()
if arg.host is None:
	arg.host = ['localhost:9200']

# Main
print(arg)
exit(0)


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
