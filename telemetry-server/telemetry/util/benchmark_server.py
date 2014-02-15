#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""Example usage:
head -25 input.txt | python benchmark_server.py "ec2-54-212-85-66.us-west-2.compute.amazonaws.com" -b 0 -p 1 --dry-run --parse-dims
cat /mnt/bench3.dims.txt | python benchmark_server.py "ec2-54-212-85-66.us-west-2.compute.amazonaws.com" -b 0 -p 8 --parse-dims
head -n 1000 input.txt | python benchmark_server.py -b 20 -p 4 --verbose --dry-run
"""

import argparse
import sys
import httplib, urllib
from urlparse import urlparse
from datetime import datetime
from multiprocessing import Process, Queue
import gzip
import StringIO
import timer

def get_partition(values, n, count):
    # split values into 'count' partitions, and return the start and end of the
    # nth partition
    num_values = len(values)
    values_per_partition = num_values / count
    leftovers = num_values % count
    start = n * values_per_partition
    end = (n + 1) * values_per_partition
    if leftovers > 0:
        start += n
        end += n
    if leftovers > n:
        end += 1
    return start, end

def send(conn, path, data, headers=None):
    conn.request("POST", path, data, headers)
    response = conn.getresponse()
    result = response.read()
#    if response.status > 201:
#        print "Got an unexpected response:", response.status
#        print result
    return result

def print_stats(label, total_mb, record_count, request_count, total_sec):
    print "%s, sent %.2fMB: %d records in %d requests in %.2f seconds: %.2fMB/s, %.2f reqs/s, %.2f records/s" % (label, total_mb, record_count, request_count, total_sec, total_mb / total_sec, request_count / total_sec, record_count / total_sec)

def run_benchmark(args):
    lines = []
    size_bytes = 0
    print "Reading input data..."
    while True:
        line = sys.stdin.readline()
        if line == '':
            break
        lines.append(line.strip())
        size_bytes += len(lines[-1])
    print "Done."

    print "Preprocessing input data..."
    for i in range(len(lines)):
        line = lines[i]
        data = ""
        dims = None
        if args.parse_dims:
            dims = line.split("\t")
            data = dims.pop()

        if dims is None or len(dims) == 0:
            dims = ["bogusid"]
            data = line
        if data and args.gzip_compress:
            size_before = len(data)
            writer = StringIO.StringIO()
            gzipper = gzip.GzipFile(fileobj=writer, mode="wb")
            gzipper.write(data)
            gzipper.close()
            data = writer.getvalue()
            writer.close()
            size_after = len(data)
            if args.verbose:
                print "Before compression, payload was", size_before, ", after:", size_after, ", ratio:", float(size_before - size_after) / size_before
        lines[i] = (dims, data)
    print "Done."

    print "Starting up", args.num_processes, "helpers"
    num_per_process = len(lines) / args.num_processes

    result_queue = Queue()
    helpers = []
    start = datetime.now()
    for i in range(args.num_processes):
        startline, endline = get_partition(lines, i, args.num_processes)
        p = Process(target=send_records,
                args=(i + 1, lines[startline:endline], args, result_queue))
        helpers.append(p)
        p.start()
    for h in helpers:
        h.join()

    results = [result_queue.get() for h in helpers]
    duration = timer.delta_sec(start)
    size_sent = 0.0
    records_sent = 0
    requests_sent = 0
    for (recs, reqs, size, sec) in results:
        size_sent += size
        records_sent += recs
        requests_sent += reqs

    size_mb = size_bytes / 1024.0 / 1024.0
    print_stats("Overall read %.2fMB" % (size_mb), size_sent, records_sent, requests_sent, duration)

def send_records(worker_id, lines, args, queue=None):
    conn = httplib.HTTPConnection(args.server_name)
    urltemplate = "/submit/telemetry/%s"
    worst_time = -1.0
    headers = {"Content-Type": "application/json; charset=UTF-8"}
    if args.gzip_compress:
        headers["Content-Encoding"] = "gzip";

    total_ms = 0.0
    record_count = 0
    request_count = 0
    total_size = 0
    #conn.set_debuglevel(1)
    batch = []
    latencies = []

    path = urltemplate % ("batch")
    if args.noop:
        path = urltemplate % ("noop")
        #path = "/"
    elif args.batch_size > 0 and args.parse_dims:
        path = urltemplate % ("batch_dims")
    # else we make a custom path for each request

    for dims, data in lines:
        record_count += 1
        #data = ""
        #dims = None
        #if args.parse_dims:
        #    dims = line.split("\t")
        #    data = dims.pop()
        #if dims is None:
        #    dims = ["bogusid"]
        #    data = line
        #print "Record", record_count, "dims:", dims

        if args.batch_size == 0:
            if not args.noop:
                path = urltemplate % ("/".join(dims))
        else:
            batch = batch + dims
            batch.append(data)
            data = ""
            if record_count % args.batch_size == 0:
                data = "\t".join(batch)
                batch = []

        if data:
            start = datetime.now()
            #print "Sending to path:", path
            if args.dry_run:
                resp = "created"
            else:
                resp = send(conn, path, data, headers)
            ms = timer.delta_ms(start)
            latencies.append(ms)
            if worst_time < ms:
                worst_time = ms
            total_ms += ms
            request_count += 1
            total_size += len(data)

            if args.verbose:
                print worker_id, "%s %.1fms, avg %.1f, max %.1f, %.2fMB/s, %.2f reqs/s, %.2f records/s" % (resp, ms, total_ms/request_count, worst_time, (total_size/1000.0/total_ms), (1000.0 * request_count / total_ms), (1000.0 * record_count / total_ms))
        if not args.verbose and record_count % 100 == 0:
            print worker_id, "Processed", record_count, "records in", request_count, "requests so far"

    # Send the last (partial) batch
    if len(batch):
        start = datetime.now()
        data = "\t".join(batch)
        if args.dry_run:
            resp = "created"
        else:
            resp = send(conn, path, data, headers)
        ms = timer.delta_ms(start)
        latencies.append(ms)
        if worst_time < ms:
            worst_time = ms
        total_ms += ms
        request_count += 1
        total_size += len(data)
        if args.verbose:
            print worker_id, "%s %.1fms, avg %.1f, max %.1f, %.2fMB/s, %.2f reqs/s, %.2f records/s" % (resp, ms, total_ms/request_count, worst_time, (total_size/1000.0/total_ms), (1000.0 * request_count / total_ms), (1000.0 * record_count / total_ms))

    latencies.sort()
    assert(len(latencies) == request_count)
    total_mb = total_size / 1024.0 / 1024.0
    total_sec = total_ms / 1000.0
    print worker_id, " %8s %8s %8s %8s %8s %8s %8s %8s" % ("Min", "Max", "Med", "Avg", "75%", "95%", "99%", "99.9%")
    print worker_id, " %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f" % (
            latencies[0],
            latencies[-1],
            latencies[int(0.5 * request_count)],
            sum(latencies) / request_count,
            latencies[int(0.75 * request_count)],
            latencies[int(0.95 * request_count)],
            latencies[int(0.99 * request_count)],
            latencies[int(0.999 * request_count)])
    print_stats(str(worker_id) + ": Including only request latency", total_mb, record_count, request_count, total_sec)
    if queue:
        queue.put((record_count, request_count, total_mb, total_sec))
    return record_count, request_count, total_size

def main():
    parser = argparse.ArgumentParser(description='Run benchmark on a Telemetry Server', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("server_name", help="The hostname[:port] of the target server to benchmark")
    parser.add_argument("-p", "--num-processes", metavar="N", help="Start N client processes", type=int, default=4)
    parser.add_argument("-b", "--batch-size", metavar="N", help="Send N records per batch (use 0 to send individual requests)", type=int, default=20)
    parser.add_argument("-z", "--gzip-compress", action="store_true")
    parser.add_argument("-m", "--parse-dims", action="store_true")
    parser.add_argument("-N", "--noop", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--dry-run", action="store_true")
    args = parser.parse_args()
    run_benchmark(args)

if __name__ == "__main__":
    sys.exit(main())
