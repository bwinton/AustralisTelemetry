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
    sysinfo = j["info"]
    ui = s["UITelemetry"]
    countableEventBuckets = []
    customizeBuckets = []

    prefix = sysinfo["OS"]

    tour_seen = "none"

    if "toolbars" in ui:
        toolbars = ui["toolbars"] 
        if not "menuBarEnabled" in toolbars: #remove weird incomplete cases
          return
        cx.write(prefix+ ",none,instances", 1)
        countableEvents = toolbars.get("countableEvents", {})
        feature_measures = {}
        #note: simple swaps in "kept"
        feature_measures["features_kept"] = toolbars.get("defaultKept",[])
        feature_measures["features_moved"] = toolbars.get("defaultMoved",[])
        feature_measures["extra_features_added"] = toolbars.get("nondefaultAdded", [])
        feature_measures["features_removed"] = toolbars.get("defaultRemoved", [])
      
        if "UITour" in ui:
          for tour in ui["UITour"]["seenPageIDs"]:
            tour_seen = tour
            cx.write(prefix + "," + tour + "," + "seenPage-" + tour, 1)
            #TODO: error checking on more than one tour

        for k,v in toolbars.iteritems():
          if k not in ["defaultKept", "defaultMoved", "nondefaultAdded", "defaultRemoved", "countableEvents", "durations"]:
            v = str(v)
            v = v.replace(",", " ") #remove commas in the tab arrays that will mess us up later
            cx.write(prefix + "," + tour_seen + ","+ k + "-" + str(v), 1)

        bucketDurations = defaultdict(list)
        durations = toolbars.get("durations",{}).get("customization",[])

        for e in durations:
            #correct for addition of firstrun buckets
            if type(e) is dict:
              bucketDurations[e["bucket"]].append(e["duration"])
            #if the default bucket is "__DEFAULT__", no tour has been seen
            else:
              bucketDurations["none"].append(e)


        for d,l in bucketDurations.items():
            bucket = "none" if d == "__DEFAULT__" else d
            for i in l:
              cx.write(prefix + "," + bucket + "," + "customization_time", i)

        #record the locations and movement of the customization items
        #write out entire set for a user(dist), 
        #and also each individual item
        for e,v in feature_measures.items():
            for item in v:
              cx.write(prefix + "," + tour_seen + "," + e+"-"+item, 1)

        #this will break pre-Australis
        for event_string in enum_paths(countableEvents,[]):
          bucket = "none" if event_string[0] == "__DEFAULT__" else event_string[0]
          cx.write(prefix+"," + bucket + "," + "-".join(event_string[1:-1]), event_string[-1])

  except Exception, e:
    print >> sys.stderr, "ERROR:", e
    print >> sys.stderr, traceback.format_exc()
    cx.write("ERROR:", str(e))

def combine(k, v, cx):
  if k == "ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  if k.endswith(" count") or k.endswith(" sum"):
    cx.write(k + " sum", sum(pymap(float,v)))
    return
  cx.write(k + " count", len(v))
  cx.write(k + " sum", sum(pymap(float,v)))

def reduce(k, v, cx):
  if k == "ERROR:":
    for i in set(v):
      cx.write(k, i)
    return

  cx.write(u",".join([k,"count"]), len(v))
  cx.write(u",".join([k,"sum"]), sum(pymap(float,v)))
