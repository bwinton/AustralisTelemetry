# Same as the osdistribution.py example in jydoop
from collections import defaultdict
import json
import sys
import traceback

pymap = map

def enum_paths(dct, path=[]):
  if not hasattr(dct, 'items'):
    path.append(dct)
    yield path
    return
  for k,v in dct.iteritems():
    for p in enum_paths(v, path + [k]):
      yield p

def map(k, d, v, cx):
  try:
    j = json.loads(v)
    if not "simpleMeasurements" in j:
      return
    s = j["simpleMeasurements"]
    if not "UITelemetry" in s:
      return
    ui = s["UITelemetry"]
    countableEventBuckets = []
    customizeBuckets = []

    prefix = "post::"
    cx.write(prefix+ "instances", 1)


    if "toolbars" in ui:
        toolbars = ui["toolbars"]

        countableDefaultEvents = toolbars["countableEvents"].get("__DEFAULT__", {})
        feature_measures = {}
        #note: simple swaps kept in "kept"
        feature_measures["features_kept"] = toolbars.get("defaultKept",[])
        feature_measures["features_moved"] = toolbars.get("defaultMoved",[])
        feature_measures["extra_features_added"] = toolbars.get("nondefaultAdded", [])
        feature_measures["features_removed"] = toolbars.get("defaultRemoved", [])
        
        bucketDurations = defaultdict(list)
        durations = toolbars.get("durations",{}).get("customization",[])

        #durations of customization and viewing tour.
        #
        #_DEFAULT_ bucket for everything but tours, which have their
        #own buckets
        for e in durations:
            bucketDurations[e["bucket"]].append(e["duration"])

        #otherwise, all times are averaged to add to the overall statistic so
        #that a single session doesn't overweight in an average
        for d,l in bucketDurations.items():
            for i in l:
              cx.write(prefix+ "duration_bucket--" + d, i)

        #record the locations and movement of the customization items
        #write out entire set for a user(dist), 
        #and also each individual item
        for e,v in feature_measures.items():
            for item in v:
                cx.write(prefix+ e+"--"+item, 1)

        for event_string in enum_paths(countableDefaultEvents,[]):
          cx.write(prefix+"-".join(event_string[:-1]), event_string[-1])
          
  except Exception, e:
    print >> sys.stderr, "ERROR:", e
    print >> sys.stderr, traceback.format_exc()
    cx.write("ERROR:", str(e))

def reduce(k, v, cx):
  if k == "ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  cx.write(k + " count", len(v))
  cx.write(k + " sum", sum(pymap(float,v)))
