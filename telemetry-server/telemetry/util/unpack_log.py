# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import sys, struct, gzip
import StringIO as StringIO
import simplejson as json
import files as fu

filename = sys.argv[1]
parse = False
if len(sys.argv) > 2 and sys.argv[2] == 'parse':
    parse = True

record_count = 0
bad_records = 0
for len_path, len_data, timestamp, path, data, err in fu.unpack(filename, verbose=True):
    record_count += 1
    if err:
        print "Record", record_count, "was bad:", err
        bad_records += 1
        continue

    if parse:
        try:
            parsed_json = json.loads(data)
        except Exception, e:
            bad_records += 1
            print "Record", record_count, "failed to parse json:", e

print "Processed", record_count, "records, with", bad_records, "bad records"
