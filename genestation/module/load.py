import sys
import json
from module.file.gff import load_gff

def main(arg, es):
	for handle in arg.descriptor:
		try:
			descriptor = json.load(handle)
			load(es, handle.name, descriptor)
		except ValueError as e:
			print("{0}: {1}".format(handle.name,e), file=sys.stderr)

def load(es, filename, descriptor):
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
	if not es.exists(index="genome", doc_type="doc", id=genome):
		es.index(index="genome", doc_type="doc", id=genome, body={
			'genus': descriptor['genus'],
			'species': descriptor['species'],
			'subspecies': descriptor['subspecies'] if 'subspecies' in descriptor else None,
			'version': descriptor['version'],
		})
	# Read GFF
	if "gff" in descriptor:
		load_gff(es, genome, filename, descriptor["gff"])

