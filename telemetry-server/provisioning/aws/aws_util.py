#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import time, os
import boto.ec2
import boto.sqs
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping
from fabric.api import *
from fabric.exceptions import NetworkError

def connect_cfg(config):
    return connect(config["region"],
                   config["aws_key"],
                   config["aws_secret_key"])

def connect(region, aws_key, aws_secret_key):
    # Use AWS keys from config
    conn = boto.ec2.connect_to_region(region,
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret_key)
    return conn

def connect_sqs_cfg(config):
    return connect_sqs(config["region"],
                   config["aws_key"],
                   config["aws_secret_key"])

def connect_sqs(region, aws_key, aws_secret_key):
    conn = boto.sqs.connect_to_region(region,
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret_key)
    return conn

def create_instance(config, aws_key=None, aws_secret_key=None):
    if aws_key is None:
        aws_key = config.get("aws_key", None)
    if aws_secret_key is None:
        aws_secret_key = config.get("aws_secret_key", None)

    conn = connect(config["region"], aws_key, aws_secret_key)
    itype = config.get("instance_type", "m1.large")
    print "Creating a new instance of type", itype
    # Known images:
    # ami-bf1d8a8f == Ubuntu 13.04
    # ami-ace67f9c == Ubuntu 13.10
    # ami-76831f46 == telemetry-base - based on Ubuntu 13.04 with dependencies
    #                 already installed
    # ami-260c9516 == telemetry-server - Based on Ubuntu 13.10, everything is
    #                 ready to go, server  will be auto-started on boot. Does
    #                 NOT auto-start process-incoming.

    # See if ephemerals have been specified
    mapping = None
    if "ephemeral_map" in config:
        mapping = BlockDeviceMapping()
        for device, ephemeral in config["ephemeral_map"].iteritems():
            mapping[device] = BlockDeviceType(ephemeral_name=ephemeral)

    reservation = conn.run_instances(
            config.get("image", "ami-bf1d8a8f"),
            key_name=config["ssl_key_name"],
            instance_type=itype,
            security_groups=config["security_groups"],
            placement=config["placement"],
            block_device_map=mapping,
            instance_initiated_shutdown_behavior=config.get("shutdown_behavior", "stop"))

    instance = reservation.instances[0]

    default_tags = config.get("default_tags", {})
    if len(default_tags) > 0:
        conn.create_tags([instance.id], default_tags)
    # TODO:
    # - find all instances where Owner = mreid and Application = telemetry-server
    # - get the highest number
    # - use the next one (or first unused one) for the current instance name.
    name_tag = {"Name": config["name"]}
    conn.create_tags([instance.id], name_tag)

    while instance.state == 'pending':
        print "Instance is pending - Waiting 10s for instance to start up..."
        time.sleep(10)
        instance.update()

    print "Instance", instance.id, "is", instance.state
    return conn, instance

def get_instance(conn, instance_id):
    reservations = conn.get_all_instances(instance_ids=[instance_id])
    instance = reservations[0].instances[0]
    return instance

# SSH is usually not available right away (the machine has to start up), so
# here we retry a few times until we can connect.
def wait_for_ssh(retries):
    for i in range(1, retries + 1):
        try:
            run("hostname")
            return True
        except NetworkError:
            print "SSH connection attempt", i, "of", retries, "failed. Trying again in 10s"
            time.sleep(10)
    return False

# Sometimes `apt-get update` fails, so retry the update/install until it works.
# Pass in a space-separated string
def install_packages(packages):
    with settings(warn_only=True):
        for i in range(1,20):
            sudo("apt-get update")
            result = sudo(" ".join(('export DEBIAN_FRONTEND=noninteractive; apt-get --yes install', packages)))
            if result.succeeded:
                break
            print "apt-get attempt", i, "failed, retrying in 2s"
            time.sleep(2)

def install_file(local_file, dest_file, dest_mode="644"):
    basename = os.path.basename(dest_file)
    tmp_location = "/tmp/" + basename
    put(local_file, tmp_location)
    sudo("mv %s %s" % (tmp_location, dest_file))
    sudo("chmod %s %s" % (str(dest_mode), dest_file))
