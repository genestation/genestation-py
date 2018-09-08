#!/usr/bin/env python3

import argparse

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

# init command parser
parser_init = subparsers.add_parser('init', description='initialize a new genestation instance')

# load command parser
parser_load = subparsers.add_parser('load', description='load genomic data into a genestation instance')
parser_load.add_argument('descriptor', nargs='+', type=argparse.FileType('r'),
	help='Genomic Data Descriptor JSON')

# get command parser
parser_get = subparsers.add_parser('get', description='get genomic data from a genestation instance')
parser_get.add_argument('index', help='ElasticSearch index')
parser_get.add_argument('id', help='Document ID')

# genome command parser
parser_genome = subparsers.add_parser('genome', description='manage genomes')
subparsers_genome = parser_genome.add_subparsers(title='subcommands', dest='subcommand')
parser_genome_show = subparsers_genome.add_parser('show', description='show genome')
parser_genome_show.add_argument('genome', help='Genome Identifier')

# Parse arguments
arg = parser.parse_args()
if arg.host is None:
	arg.host = ['localhost:9200']

# Main
if arg.command == "init":
	from module.init import main
	main(arg)
elif arg.command == "load":
	from module.load import main
	main(arg)
elif arg.command == "get":
	from module.get import main
	main(arg)
elif arg.command == "genome":
	from module.genome import main
	main(arg)
