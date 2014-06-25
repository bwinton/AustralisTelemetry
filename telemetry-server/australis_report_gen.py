#!/usr/bin/python

import argparse
import simplejson as json
import os
from collections import namedtuple
import traceback
import sys
import telemetry.util.timer as timer
from analysis.process_output import process_output
from datetime import datetime, timedelta
from urllib import urlopen
import subprocess

from mapreduce.job import Job
try:
    from boto.s3.connection import S3Connection
    BOTO_AVAILABLE=True
except ImportError:
    BOTO_AVAILABLE=False

OUTPUT_DIR_BASE = "../analysis/output"

#weeks start at 1
def get_week_endpoints(week_no, year=2014):
  year_start = datetime(year,1,1).date()
  first_tues = year_start + timedelta(days=((8 - year_start.weekday()) % 7))

  if type(week_no) is int:
    start = first_tues + timedelta(days=7*(int(week_no)-1))
    end = start + timedelta(days=6) #endpoints are inclusive

  elif week_no == "current": #return previous complete week. If today is tuesday, takes previous week
    last_full_day = datetime.today().date() - timedelta(days=1)
    last_full_mon = last_full_day - timedelta(days=(last_full_day.weekday()%7))
    end = last_full_mon #end on a monday!
    start = end - timedelta(days=6)

  else:
    return("00000000", "99999999")

  return (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))

def get_current_weekno(year=2014):
  year_start = datetime(year,1,1)

  first_tues = year_start + timedelta(days=((8 - year_start.weekday()) % 7))
  today = datetime.today()
  return int((today - first_tues).days / 7) + 1

def curr_version(channel):
  if channel == "nightly":
    channel = "central"
  today = datetime.today().date()
  releases_site = urlopen("http://latte.ca/cgi-bin/status.cgi")
  releases = json.loads(releases_site.read())

  for i,r in enumerate(releases):
    if datetime.strptime(r["sDate"], "%Y-%m-%d").date() > today:
      if i == 0:
        raise Exception("today's date too early for dataset")
      return releases[i-1]["data"][channel].split()[1]
  raise Exception("today's date too late for dataset")
      


#many fields faked out. keep this way for now.
def generate_filters(args, output_file):
    chans = {key: args.__dict__[key] for key in ("nightly", "aurora", "beta", "release")}
    channels = filter(lambda x: args.__dict__[x] is True, chans)
    
    version_min, version_max = None, None

    if type(args.version) is int:
      version_min = str(args.version) + ".0"
      version_max = str(args.version) + ".999"
    elif len(channels) == 1 and args.version == "current":
      args.version = curr_version(channels[0])
      version_min = str(args.version) + ".0"
      version_max = str(args.version) + ".999"
    else:
      version_min = "0.0"
      version_max = "999.999"

    start, end = get_week_endpoints(args.week)

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
        "min": str(version_min),
        "max": str(version_max)
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
    with open(output_file, "w") as outfile:
        json.dump(fltr, outfile)

    return output_file

#many of these args can be exposed at the command line. no need for now.
def run_mr(filter, output_file, local_only):

  args = {
    "job_script" : "../uitour.py",
    "input_filter": filter,
    "num_mappers" : 16,
    "num_reducers" : 4,
    "data_dir" : "../work/cache",
    "work_dir" : "../work",
    "output" : output_file,
    "bucket" : "telemetry-published-v1",
    "local_only" : local_only 
  }

  if not args["local_only"]:
      if not BOTO_AVAILABLE:
          print "ERROR: The 'boto' library is required except in 'local-only' mode."
          print "       You can install it using `sudo pip install boto`"
          parser.print_help()
          return -2

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
  return (exit_code, output_file)

parser = argparse.ArgumentParser()
parser.add_argument("-w", "--week", help="enter week number of 2014 to analyze")
parser.add_argument("--nightly", action="store_true", help="Use flag to include channel")
parser.add_argument("--aurora", action="store_true", help="Use flag to include channel")
parser.add_argument("--beta", action="store_true", help="Use flag to include channel")
parser.add_argument("--release", action="store_true", help="Use flag to include channel")
parser.add_argument("-v", "--version", help="enter version")
parser.add_argument("-t", "--tag", help="enter a label to identify the data run")
parser.add_argument("--local-only", action="store_true", dest = "local_only", help="use flag to run using local data")

args = parser.parse_args()

current_dir = sys.path[0]

if not args.tag:
  if args.week != "current":
    args.tag = "2014_" + args.week
  elif args.week == "current":
    args.tag = "2014_" + str(get_current_weekno())

output_dir = "/".join([current_dir,OUTPUT_DIR_BASE,args.tag]) + "/"
# proc = subprocess.Popen(["ls","/".join([current_dir,OUTPUT_DIR_BASE,args.tag, PIPE=stdout])])
# print proc.communicate()
proc = subprocess.Popen(["mkdir","/".join([current_dir,OUTPUT_DIR_BASE,args.tag])])
proc.wait()

filterfile = generate_filters(args, output_dir + "filter.json" )
error, mr_file = run_mr(filterfile, output_dir + "mr_output.csv", args.local_only)
process_output(open(mr_file), output_dir + "out.csv")



  

