#!/usr/bin/python

'''
-default before/after australis?
- generate filters
- generate two run.sh
- run uitour.py/output, uitour_peruser.py/output_peruser
- run processoutput/processoutput_peruser
- generate stats
'''

import argparse
import json
import os
from telemetry-server.mapreduce.job import Job


FILTER_DIR = "./"


#fwhat input do we want??? incomplete
def generate_filters(args):
    chans = {key: args.__dict__[key] for key in ("nightly", "aurora", "beta", "release")}
    channels = filter(lambda x: args.__dict__[x] is True, chans)

    fltr = {
    "version": 1,
    "dimensions": [
    {
      "field_name": "reason",
      "allowed_values": ["saved-session"]
    },
    {
      "field_name": "appName",
      "allowed_values": ["Firefox"]
    },
    {
      "field_name": "appUpdateChannel",
      "allowed_values": channels
    },
    {
      "field_name": "appVersion",
      "allowed_values": {
        "min": "0.0",
        "max": "999.999"
      }
    },
    {
      "field_name": "appBuildID",
      "allowed_values": {
        "min": "20140123004002",
        "max": "99999999999999"
      }
    },
    {
      "field_name": "submission_date",
      "allowed_values": {
        "min": "20140203",
        "max": "20140321"
      }
    }
    ]
    }
    filterfile = FILTER_DIR + "auto_filter.json"
    with open(filterfile, "w") as outfile:
        json.dump(fltr, outfile)
    return filterfile

def run_mr(mr_file, filter, output_file):
    cmd = " ".join(["python -m telemetry-server/mapreduce.job", mr_file, "--input-filter", filter, "--num-mappers 16 --num-reducers 4, --data-dir ./work/cache --work-dir ./work --output", output_file, "$PULL --bucket \"telemetry-published-v1\""])
    j = Job()
    print j

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--start", help="enter start date in the format YYYYMMDD, ex: 20140203", required=True)
parser.add_argument("-e", "--end", help="enter end date in the format YYYYMMDD, ex: 20140203", required=True)
parser.add_argument("--nightly", action="store_true", help="Use flag to include channel")
parser.add_argument("--aurora", action="store_true", help="Use flag to include channel")
parser.add_argument("--beta", action="store_true", help="Use flag to include channel")
parser.add_argument("--release", action="store_true", help="Use flag to include channel")

args = parser.parse_args()

filterfile = generate_filters(args)

#maybe set some of these up top
run_mr("./uitour.py", filterfile, "../mr_output.out")


