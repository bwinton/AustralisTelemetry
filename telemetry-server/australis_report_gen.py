#!/usr/bin/python

'''
-default AFTER australis?
- generate filters
- generate two run.sh
- run uitour.py/output, uitour_peruser.py/output_peruser
- run processoutput/processoutput_peruser
- generate stats
'''

import argparse
import simplejson as json
import os
from collections import namedtuple
from datetime import datetime
import traceback
import sys
import telemetry.util.timer as timer
from analysis.process_output import process_output
from datetime import datetime, timedelta

from mapreduce.job import Job

FILTER_DIR = "./"

#weeks start at 1
def get_week_endpoints(week_no, year=2014):
  year_start = datetime(year,1,1).date()
  first_tues = year_start + timedelta(days=((8 - year_start.weekday()) % 7))

  if week_no is not None:
    start = first_tues + timedelta(days=7*(week_no-1))
    end = start + timedelta(days=6) #endpoints are inclusive

  else: #return previous complete week. If today is tuesday, takes previous week
    last_full_day = datetime.today().date() - timedelta(days=1)
    last_full_mon = last_full_day - timedelta(days=(last_full_day.weekday()%7))
    end = last_full_mon #end on a monday!
    start = end - timedelta(days=6)


  return (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))


#many fields faked out. keep this way for now.
def generate_filters(args):
    chans = {key: args.__dict__[key] for key in ("nightly", "aurora", "beta", "release")}
    channels = filter(lambda x: args.__dict__[x] is True, chans)
    
    start, end = get_week_endpoints(args.week)
    print start, end
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
        "min": str(args.version) + ".0",
        "max": str(args.version) + ".999"
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
        "min": start,
        "max": end
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

#for now, only int vals for version. allow sub-versions in future?
parser = argparse.ArgumentParser()
parser.add_argument("-w", "--week", help="enter week number of 2014 to analyze", type=int)
parser.add_argument("--nightly", action="store_true", help="Use flag to include channel")
parser.add_argument("--aurora", action="store_true", help="Use flag to include channel")
parser.add_argument("--beta", action="store_true", help="Use flag to include channel")
parser.add_argument("--release", action="store_true", help="Use flag to include channel")
parser.add_argument("-v", "--version", help="enter version", type = int, required=True)

args = parser.parse_args()

filterfile = generate_filters(args)
outfile = "./out_test.out"

#TODO: return successful outfile here?
run_mr(filterfile, outfile)

process_output(open(outfile), "out.csv")

  

