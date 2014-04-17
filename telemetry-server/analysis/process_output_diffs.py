import fileinput
from collections import defaultdict
from pprint import pprint as pp
import operator

features = defaultdict(lambda: defaultdict(int))
instances = {}



for line in fileinput.input():
	pre_file = fileinput.filename().endswith("pre.out")
	if line.startswith("ERROR"):
		continue
	if line.startswith("visibleTabs") or line.startswith("hiddenTabs"):
		continue
	elif line.startswith("bucket"):
		tokens = line.split("-c", 1)
		tokens[1] = "c" + tokens[1]
		line = tokens[1]

	if not line.startswith("instances"):
		tokens = line.split()
		item = tokens[0]
		agg = tokens[1]
		val = tokens[2]

		if agg == "count":
			#what's the way of doing this??
			if pre_file:
				features[item]["pre"] = float(val)
			else:
				features[item]["post"] = float(val)
	else:
		if pre_file:
			instances["pre"] = float(line.split()[-1])
		else:
			instances["post"] = float(line.split()[-1])

for f in sorted(features):
	if "pre" in features[f].keys() and "post" in features[f].keys():
		pre = round(features[f]["pre"]*100/instances["pre"], 2)
		post = round(features[f]["post"]*100/instances["post"], 2)
		diff = post - pre
		if diff >= 10 or (pre > 0 and diff/pre >= .5):
			marker = "***"
		else:
			marker = ""
		print marker, f, "\t", str(pre)+"%", str(post) + "%", "\t", str(post - pre) + "%"
