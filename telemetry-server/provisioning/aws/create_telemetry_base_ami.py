#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Example invocation:
# $ cd /path/to/telemetry-server
# $ python -m provisioning.aws.create_telemetry_base_ami -k "my_aws_key" -s "my_aws_secret" provisioning/aws/telemetry_server_base.pv.json

from launch_telemetry_server import TelemetryServerLauncher
from create_ami import AmiCreator
import sys
import traceback

def main():
    launcher = TelemetryServerLauncher()
    creator = AmiCreator(launcher)
    try:
        result = creator.create('Pre-loaded image for telemetry nodes. Knows ' \
                                'how to run all the core services, but does ' \
                                'not auto-start them on boot.')
        return result
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
