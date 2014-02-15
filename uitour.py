# Same as the osdistribution.py example in jydoop
import json

def map(k, d, v, cx):
  try:
    j = json.loads(v)
    if not "simpleMeasurements" in j:
      cx.write("ERROR: MISSING SIMPLEMEASURES", 1)
      return
    if not "UITelemetry" in k["simpleMeasurements"]:
      cx.write("ERROR: MISSING UITELEMETRY", 1)
      return

    uitelemetry = j['simpleMeasurements']['UITelemetry']
    try:
      ui = json.loads(uitelemetry)
      cx.write(k, uitelemetry)
    except:
      cs.write("ERROR: JSON PARSE ERROR 2", 1)
  except:
    cx.write("ERROR: JSON PARSE ERROR", 1)

def reduce(k, v, cx):
  if k.startswith("ERROR: "):
    cx.write(k, sum(v))
    return
  cx.write(k, v)
