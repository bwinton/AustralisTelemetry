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

from collections import defaultdict
import fileinput
import operator
from pprint import pprint as pp
import sys

features = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
instances = None

for line in fileinput.input():
    if line.startswith("ERROR"):
        print >> sys.stderr, line
    elif (line.startswith("click") or
          line.startswith("customize") or
          line.startswith("seenPage")):
        tokens = line.split("-", 1)
    elif line.startswith("bucket") or line.startswith("__DEFAULT__"):
        tokens = line.split("-c", 1)
        tokens[1] = "c" + tokens[1]
        del tokens[0]
        tokens = tokens[0].split("-", 1)
    else:
        tokens = line.split("--", 1)
    category = tokens[0]
    if not category.startswith("instances"):
        obj = tokens[1].split(" ", 1)[0]
        val = tokens[1].split()
        if val[1] in ("sum", "count"):
            features[category][obj][val[1]] = float(val[2])
    else:
        instances = float(tokens[0].split()[-1])

print "item,subitem,avg. instances per session,pct sessions with occurrence"
for category, cat_fs in features.iteritems():
    for f in sorted(cat_fs, key=cat_fs.get, reverse=True):
        avg = str(round(cat_fs[f]["sum"]/instances, 4))
        pct = str(round(cat_fs[f]["count"]/instances, 4))
        print category + "," + f + "," + avg + "," + pct
