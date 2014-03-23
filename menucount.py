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
        countableEvents = toolbars["countableEvents"]
    for bucket in countableEvents:
      events = countableEvents[bucket]
      for event in events:
        if "button" in events[event] and "left" in events[event]["button"]:
          data = {
            "count": events[event]["button"]["left"],
            "uptime": s["uptime"]
          }
          cx.write(event + "-" + j["info"]["appVersion"], data)

  except Exception, e:
    cx.write("JSON PARSE ERROR:", e)

def reduce(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  # v = [{count: 123, uptime: 456}, {count: 789, uptime: 101112}, ...]
  counts = 0
  uptime = 0
  for item in v:
    print type(item), item
    counts += item["count"]
    uptime += item["uptime"]
  cx.write(k + "-length", len(v))
  cx.write(k + "-count", counts)
  cx.write(k + "-uptime", uptime)
  cx.write(k + "-scaled", counts * 1.0 / uptime)
