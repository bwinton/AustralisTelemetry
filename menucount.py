import json

# Check click-menu-button vs click-appmenu usage.

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
            "length": 1,
            "count": events[event]["button"]["left"],
            "uptime": s["uptime"]
          }
          cx.write(event + "-" + j["info"]["appVersion"], data)

  except Exception, e:
    cx.write("JSON PARSE ERROR:", e)

def combine(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  # v = [{length: 1, count: 123, uptime: 456}, {length: 1, count: 789, uptime: 101112}, ...]

  summary = {
    "length": 0,
    "count": 0,
    "uptime": 0
  }
  for item in v:
    summary["length"] += item["length"]
    summary["count"] += item["count"]
    summary["uptime"] += item["uptime"]
  cx.write(k, summary)

def reduce(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  # v = [{length: 3, count: 123, uptime: 456}, {length: 50, count: 789, uptime: 101112}, ...]
  lengths = 0
  counts = 0
  uptime = 0
  for item in v:
    lengths += item["length"]
    counts += item["count"]
    uptime += item["uptime"]
  cx.write(k + "-length", lengths)
  cx.write(k + "-count", counts)
  cx.write(k + "-uptime", uptime)
  cx.write(k + "-scaled", counts * 1.0 / uptime)
