import csv
import json

def csv_to_gat(infile, outfile, columns, csvparam):
	# Interpret column specification
	columns = [(
		column[0] if type(column[0]) is list else [column[0]], # input column header(s)
		column[1] if len(column) > 1 and column[1] is not None # output column header
			else column[0][0] if type(column[0]) is list else column[0], # defaults to first input column
		column[2] if len(column) > 2 and column[2] is not None # column transformation
			else lambda *x: None if len(x) == 0 else x[0] if len(x) == 1 else x,
			# defaults to null (argc==0) or the input string (argc==1) or string array (argc>1)
	) for column in columns]

	# Convert to GAT
	with open(infile) as csvfile, open(outfile, 'w', newline='\n') as gatfile:
		csvreader = csv.reader(csvfile, **csvparam)
		header = next(csvreader)
		try:
			column_idx = [[header.index(part) for part in column[0]] for column in columns]
		except ValueError as e:
			e.args = ('\'{0}\' column does not exist in "{1}"'.format(e.args[0].split('\'')[1], infile),)
			raise e
		# Write header section

		# Write column names
		gatfile.write('\t'.join([column[1] for column in columns])+'\n')
		for lineno, row in enumerate(csvreader,2):
			try:
				gatfile.write('\t'.join([
					json.dumps(columns[col_idx][2](*[row[idx] for idx in file_idxs]))
					for col_idx, file_idxs in enumerate(column_idx)
				])+'\n')
			except Exception as e:
				e.args = ('{0}:{1}: {2}\n{3}'.format(infile,lineno,e.args[0],row),)
				raise e
