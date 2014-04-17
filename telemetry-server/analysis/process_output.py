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
	elif line.startswith("bucket"):
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
		if val[1] == "count":
			features[category][obj] =float(val[2])
	else:
		instances = float(tokens[0].split()[-1])

for category, cat_fs in features.iteritems():
	print category

	for f in sorted(cat_fs,key = cat_fs.get, reverse = True):
		print "\t", f, "\t", str(round(cat_fs[f]*100/instances, 2))+"%"
