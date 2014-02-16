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
    cx.write(k, json.dumps(ui))

  except Exception, e:
    cx.write("JSON PARSE ERROR:", e)

def reduce(k, v, cx):
  if k == "JSON PARSE ERROR:":
    for i in set(v):
      cx.write(k, i)
    return
  cx.write(k, v)