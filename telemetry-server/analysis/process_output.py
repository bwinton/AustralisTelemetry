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

import fileinput
from collections import defaultdict
from pprint import pprint as pp
import operator

features = defaultdict(lambda: defaultdict(int))
instances = None

for line in fileinput.input():
	if line.startswith("ERROR"):
		continue
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
		if val[1] == "count":
			features[category][obj] =float(val[2])
	else:
		instances = float(tokens[0].split()[-1])

print "item,subitem,instances per session,percentage of sessions with occurrence"
for category, cat_fs in features.iteritems():
	for f in sorted(cat_fs,key = cat_fs.get, reverse = True):
		print category + "," + f + "," +str(cat_fs[f])+"," +str(round(cat_fs[f]/instances, 4))