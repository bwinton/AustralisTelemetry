''' 
post-process what comes out of m/r job

usage: cat final_data/my_mapreduce_results_pre.out | python process_output.py 

input: output from uitour, ex:
visibleTabs--[221, 1] count	1
visibleTabs--[221, 1] sum	1.0
click-builtin-item-alltabs-button-left count	422

output: csv w/ header listing
item,subitem,instances per session,percentage of sessions with occurrence

ex:
bookmarksBarEnabled,False,194609.0,0.5405
bookmarksBarEnabled,True,165417.0,0.4595
'''

def process_output(filecontents, outfile):
	from collections import defaultdict
	from pprint import pprint as pp
	import operator
	import csv

	features = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))
	instances = None

	for fullline in filecontents:
		if fullline.startswith("ERROR"):
			continue

		#deal with prefixes
		prefix, line = fullline.split("&&", 1)

		if line.startswith("click"):
			tokens = line.split("-", 1)
		elif line.startswith("customize"):
			tokens = line.split("-", 1)
		elif line.startswith("bucket") or line.startswith("__DEFAULT__"):
			tokens = line.split("-c", 1)
			tokens[1] = "c" + tokens[1]
			del tokens[0]
			tokens = tokens[0].split("-", 1)
		elif line.startswith("seenPage"):
			tokens = line.split("-",1)
		else:
			tokens = line.split("--", 1)
		category = tokens[0]
		if not category.startswith("instances"):
			obj = tokens[1].split(" ", 1)[0]
			val = tokens[1].split()
			if val[1] == "sum":
				features[prefix][category][obj]["sum"] = float(val[2])
			elif val[1] == "count":
				features[prefix][category][obj]["count"] = float(val[2])
		else:
			instances = float(tokens[0].split()[-1])

	with open(outfile, "w") as outfile:
		csvwriter = csv.writer(outfile)
		csvwriter.writerow(["sys_info", "item","subitem","avg. instances per session","pct sessions with occurrence"])
		for prefix, agg_features in features.iteritems():
			for category, cat_fs in agg_features.iteritems():
				for f in sorted(cat_fs,key = cat_fs.get, reverse = True):
					csvwriter.writerow([prefix,category,f,str(round(cat_fs[f]["sum"]/instances, 4)),str(round(cat_fs[f]["count"]/instances, 4))])

if __name__ == '__main__':
	import fileinput
	import sys

	process_output(sys.stdout)



