import json
import sys
import traceback

# Check click-menu-button vs click-appmenu usage.

def getButtonData(data):
  rv = None
  if "left" in data:
    left = data["left"]
    if left:
      return {
        "length": 1,
        "count": left,
      }
  return rv

def handleMultipleButtons(event):
  rv = []
  for key in event:
    data = getButtonData(event[key])
    if data:
      rv.append([key, data])
  return rv

def handleSingleButton(event):
  rv = []
  data = getButtonData(event["button"])
  if data:
    rv.append([None, data])
  return rv

def handleDirectNumbers(event):
  rv = []
  for key in event:
    rv.append([key, {
      "length": 1,
      "count": event[key],
    }])
  return rv


def defaultHandler(event):
  return None

handlers = {
  "click-bookmarks-menu-button": handleMultipleButtons,
  "click-menu-button": handleSingleButton,
  "click-builtin-item": handleMultipleButtons,
  "customize": handleDirectNumbers
}

def handleEvents(events, cx, uptime, version):
  for event in events:
    handler = handlers.get(event, defaultHandler)
    rv = handler(events[event])
    if rv:
      for extra, data in rv:
        key = event
        if extra:
          key += "-" + extra
        key += "-" + version
        data["uptime"] = uptime
        cx.write(key, json.dumps(data))

def map(k, d, v, cx):
  try:
    j = json.loads(v)
    if not "simpleMeasurements" in j:
      return
    s = j["simpleMeasurements"]
    if not "UITelemetry" in s:
      return
    ui = s["UITelemetry"]
    countableEvents = {}
    if "toolbars" in ui:
      cx.write("session-count-" + j["info"]["appVersion"], 1)
      cx.write("session-uptime-" + j["info"]["appVersion"], s["uptime"])
      toolbars = ui["toolbars"]
      if "countableEvents" in toolbars:
        countableEvents = toolbars["countableEvents"]
        for bucket in countableEvents:
          handleEvents(countableEvents[bucket], cx,
            s["uptime"], j["info"]["appVersion"])
  except Exception, e:
    print >> sys.stderr, "BW2", e
    print >> sys.stderr, traceback.format_exc()
    cx.write("JSON PARSE ERROR:", str(e))

def combine(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return

  if k.startswith("session-"):
    cx.write(k, sum(v))
    return
  # v = [{length: 1, count: 123, uptime: 456}, {length: 1, count: 789, uptime: 101112}, ...]

  summary = {
    "length": 0,
    "count": 0,
    "uptime": 0
  }
  for item in v:
    # print >> sys.stderr, "BW3", item
    item = json.loads(item)
    summary["length"] += item["length"]
    summary["count"] += item["count"]
    summary["uptime"] += item["uptime"]
  cx.write(k, json.dumps(summary))

def reduce(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return

  if k.startswith("session-"):
    cx.write(k, sum(v))
    return

  # v = [{length: 3, count: 123, uptime: 456}, {length: 50, count: 789, uptime: 101112}, ...]
  lengths = 0
  counts = 0
  uptime = 0
  for item in v:
    # print >> sys.stderr, "BW4", item
    item = json.loads(item)
    lengths += item["length"]
    counts += item["count"]
    uptime += item["uptime"]
  cx.write(k + "-length", lengths)
  cx.write(k + "-count", counts)
  cx.write(k + "-uptime", uptime)
  if uptime > 0:
    cx.write(k + "-scaled", counts * 1.0 / uptime)
  else:
    cx.write(k + "-scaled", "Divide by 0");
