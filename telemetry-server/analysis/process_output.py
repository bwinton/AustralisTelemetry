''' 
post-process what comes out of m/r job

usage: cat final_data/my_mapreduce_results_pre.out | python process_output.py 

input: output from uitour, ex:
Linux,none,features_kept-bookmarks-menu-button,sum	2.0
WINNT,bucket_UITour|australis-tour-aurora-29.0a2,click-builtin-item-preferences-button-left,count	1

output: csv w/ header listing
osinfo,item,instances per session,percentage of sessions with occurrence

ex:
WINNT,none,bookmarksBarEnabled-False,194609.0,0.5405
Linux,tour-29,bookmarksBarEnabled-True,165417.0,0.4595
'''

#position data
PREFIX=0
BUCKET=1
ITEM=2
COUNT=3



def process_output(filecontents, outfile):
	from collections import defaultdict
	from pprint import pprint as pp
	import operator
	import csv

	counts = defaultdict(lambda: defaultdict(int))
	instances = {}

	for line in filecontents:
		if line.startswith("ERROR"):
			continue

		tokens = line.split(",")
		tokens[COUNT] = tokens[COUNT].split()
		if tokens[ITEM] == "instances" and tokens[COUNT][0] == "count":
			instances[tokens[PREFIX]] = float(tokens[COUNT][1])

		#right now, only look at counts with all fields included
		counts[tuple(tokens[PREFIX:ITEM+1])][tokens[COUNT][0]] = tokens[COUNT][1]

	with open(outfile, "w") as outfile:
		csvwriter = csv.writer(outfile)
		csvwriter.writerow(["sys_info","tour_bucket", "item","instances_per_session","percent_sessions_with_occurrence"])
		for tup, cts in counts.iteritems():
			output = list(tup)
			instances_per_session = round(float(cts["sum"])/instances[tup[PREFIX]], 3)
			pct_with_occurrence = round(float(cts["count"])/instances[tup[PREFIX]], 3)
			output.extend([instances_per_session, pct_with_occurrence])
			csvwriter.writerow(output)

if __name__ == '__main__':
	import fileinput
	import sys

	process_output(sys.stdout)



