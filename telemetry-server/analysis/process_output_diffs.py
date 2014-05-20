from collections import defaultdict
import fileinput
import operator
from pprint import pprint as pp
import sys

features = defaultdict(lambda: defaultdict(int))
instances = {}


for line in fileinput.input():
    prepost_key = "post"
    if fileinput.filename().endswith("pre.out"):
        prepost_key = "pre"
    if line.startswith("ERROR"):
        print >> sys.stderr, line
    if line.startswith("visibleTabs") or line.startswith("hiddenTabs"):
        continue
    elif line.startswith("bucket"):
        tokens = line.split("-c", 1)
        tokens[1] = "c" + tokens[1]
        line = tokens[1]

    if not line.startswith("instances"):
        (item, agg, val) = line.split()[:3]
        if agg == "count":
            features[item][prepost_key] = float(val)
    else:
        instances[prepost_key] = float(line.split()[-1])

for f in sorted(features):
    if "pre" in features[f].keys() and "post" in features[f].keys():
        pre = round(features[f]["pre"] * 100 / instances["pre"], 2)
        post = round(features[f]["post"] * 100 / instances["post"], 2)
        diff = post - pre
        if diff >= 10 or (pre > 0 and diff/pre >= .5):
            marker = "***"
        else:
            marker = ""
        print (marker, f, "\t", str(pre)+"%", str(post) + "%",
               "\t", str(post - pre) + "%")
