#!/usr/bin/env python3

import argparse
from elasticsearch import Elasticsearch

#import logging
#logging.basicConfig()
#logging.getLogger('elasticsearch').setLevel(logging.DEBUG)
#logging.getLogger('urllib3').setLevel(logging.DEBUG)

PROG='genestation'
VERSION="0.0.1"

# Argparse
parser = argparse.ArgumentParser(prog=PROG)
parser.add_argument('--version', action='version',
	version='%(prog)s '+VERSION)
parser.add_argument('--host', action='append',
	help='url(s) to the elasticsearch node; default localhost:9200')
subparsers = parser.add_subparsers(
	title='commands', dest='command')

# init command
parser_init = subparsers.add_parser('init', description='initialize a new genestation instance')

# load command
parser_load = subparsers.add_parser('load', description='load genomic data')
parser_load.add_argument('descriptor', nargs='+', type=argparse.FileType('r'),
	help='Genomic Data Descriptor JSON')

# index command
parser_index = subparsers.add_parser('index', description='work with indices')
subparsers_index = parser_index.add_subparsers(title='subcommands', dest='subcommand')
parser_index_show = subparsers_index.add_parser('show', description='show metadata about the index')
parser_index_show.add_argument('index', help='index to show')
parser_index_makestats = subparsers_index.add_parser('make-stats', description='(re)create stats index')
parser_index_makestats.add_argument('index', help='index to stat')

# get command
parser_get = subparsers.add_parser('get', description='get genomic data')
parser_get.add_argument('index', help='ElasticSearch index')
parser_get.add_argument('id', help='Document ID')

# search command
parser_search = subparsers.add_parser('search', description='search genomic data')
parser_search.add_argument('index', help='ElasticSearch index')
parser_search.add_argument('query', help='search query')

# genome command
parser_genome = subparsers.add_parser('genome', description='manage genomes')
subparsers_genome = parser_genome.add_subparsers(title='subcommands', dest='subcommand')
parser_genome_show = subparsers_genome.add_parser('show', description='show genome')
parser_genome_show.add_argument('genome', help='Genome Identifier')

# Parse arguments
arg = parser.parse_args()
if arg.host is None:
	arg.host = ['localhost:9200']

# Test connection
es = Elasticsearch(arg.host,timeout=600000)
if es.ping() is False:
	print("Cannot access ElasticSearch at", arg.host)
	exit(1)
# ElaticSearch initialization
if arg.command == "init":
	from module.init import main
	main(arg, es)
	exit(0)
if not es.indices.exists(index='genome'):
	print("Genome Index is not initialized", file=sys.stderr)
	exit(1)

# Main
if arg.command == "load":
	from module.load import main
	main(arg, es)
elif arg.command == "index":
	from module.index import main
	main(arg, es)
elif arg.command == "get":
	from module.get import main
	main(arg, es)
elif arg.command == "search":
	from module.search import main
	main(arg, es)
elif arg.command == "genome":
	from module.genome import main
	main(arg, es)
