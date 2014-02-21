# Same as the osdistribution.py example in jydoop
import json

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
    if "toolbars" in ui:
      toolbars = ui["toolbars"]
      if "countableEvents" in toolbars:
        countableEventBuckets = toolbars["countableEvents"].keys()
      if "durations" in toolbars and "customization" in toolbars["durations"]:
        customizeBuckets = toolbars["durations"]["customization"]
    countableEventBuckets = [item for item in countableEventBuckets
                                  if not item.startswith(u"click-")]
    countableEventBuckets = [item for item in countableEventBuckets
                                  if not item == u"__DEFAULT__"]
    cx.write("uptime", s["uptime"])
    for item in customizeBuckets:
      cx.write("custo-" + json.dumps(item["bucket"]), item["duration"])
    for bucket in countableEventBuckets:
      cx.write("event-" + json.dumps(bucket), 1)

  except Exception, e:
    cx.write("JSON PARSE ERROR:", e)

def reduce(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  cx.write(k + "count", len(v))
  cx.write(k + "sum", sum(v));
