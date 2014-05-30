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
from collections import namedtuple
from datetime import datetime
import traceback
import sys
import telemetry.util.timer as timer

from mapreduce.job import Job

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
        "min": "29.0",
        "max": "29.999"
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
        "max": "20140429"
      }
    }
    ]
    }
    filterfile = FILTER_DIR + "filter-auto.json"
    with open(filterfile, "w") as outfile:
        json.dump(fltr, outfile)

    return filterfile

#many of these args can be exposed at the command line. no need for now.
def run_mr(filter, output_file):
  
  args = {
    "job_script" : "../uitour.py",
    "input_filter": filter,
    "num_mappers" : 16,
    "num_reducers" : 4,
    "data_dir" : "../work/cache",
    "work_dir" : "../work",
    "output" : output_file,
    "bucket" : "telemetry-published-v1",
    "local_only" : True
  }


    # if not args.local_only:
    #     if not BOTO_AVAILABLE:
    #         print "ERROR: The 'boto' library is required except in 'local-only' mode."
    #         print "       You can install it using `sudo pip install boto`"
    #         parser.print_help()
    #         return -2
    #     # If we want to process remote data, some more arguments are required.
    #     for remote_req in ["bucket"]:
    #         if not hasattr(args, remote_req) or getattr(args, remote_req) is None:
    #             print "ERROR:", remote_req, "is a required option"
    #             parser.print_help()
    #             return -1

  job = Job(args)
  start = datetime.now()
  exit_code = 0
  try:
      job.mapreduce()
  except:
      traceback.print_exc(file=sys.stderr)
      exit_code = 2
  duration = timer.delta_sec(start)
  print "All done in %.2fs" % (duration)
  return exit_code


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--start", help="enter start date in the format YYYYMMDD, ex: 20140203", required=True)
parser.add_argument("-e", "--end", help="enter end date in the format YYYYMMDD, ex: 20140203", required=True)
parser.add_argument("--nightly", action="store_true", help="Use flag to include channel")
parser.add_argument("--aurora", action="store_true", help="Use flag to include channel")
parser.add_argument("--beta", action="store_true", help="Use flag to include channel")
parser.add_argument("--release", action="store_true", help="Use flag to include channel")

args = parser.parse_args()

filterfile = generate_filters(args)
outfile = "./out_test.out"

run_mr(filterfile, outfile)

