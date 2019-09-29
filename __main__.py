#!/usr/bin/env python3
import paho.mqtt.client as mqtt
from subprocess import check_output, call
from time import sleep
import json
import os
import sys
import datetime
import logging
import argparse
import socket


# Default settings
settings = {
    "cifs": {
        "user": "",
        "password": "",
        "share": "//10.0.0.2/backup",
        "target": "/media/backup",
        "subDir": ""
    },
    "backup": {
        "command": "/opt/bkup_rpimage/bkup_rpimage.sh",
        "image_base_name": "sdimage",
        "full_backup_interval": "monthly"
    },
    "mqtt" : {
        "client_id": "rpi-backup",
        "host": "127.0.0.1",
        "port": 1883,
        "keepalive": 60,
        "bind_address": "",
        "username": None,
        "password": None,
        "qos": 0,
        "pub_topic_namespace": "pi/backup",
        "retain": True
    }
}


#
# FUNCTIONS
#

def mqtt_publish(topic, payload):
    # Send out messages to the MQTT broker
    log.debug('MQTT pub: {}: {}'.format(topic, payload))
    mqtt_client.publish(
        topic = namespace + '/' + topic,
        payload = payload,
        qos = settings['mqtt']['qos'],
        retain = settings['mqtt']['retain'])

def fatal_error(message, error_code = 1, unmount = None):
    # Publish error messages on MQTT and logs
    mqtt_publish(
        topic = "last_error/message",
        payload = message)
    mqtt_publish(
        topic = "last_error/timestamp",
        payload = str(datetime.datetime.now()))
    log.fatal('{} [ERR{}]'.format(
        message,
        str(error_code)))
    # Try to unmount cleanly
    if unmount != None:
        call('umount {}'.format(unmount), shell=True)
    # Stop script
    sys.exit(error_code)


#
# CONFIGURATION
#

# Parse arguments
parser = argparse.ArgumentParser(description="Python bkup_rpimage wrapper and MQTT bridge")
parser.add_argument("-c", "--config", default="config.json", help="Configuration file (default: %(default)s)")
parser.add_argument("-l", "--loglevel", default="INFO", help="Event level to log (default: %(default)s)")
args = parser.parse_args()

# Parse log level
num_level = getattr(logging, args.loglevel.upper(), None)
if not isinstance(num_level, int):
    raise ValueError('Invalid log level: %s' % args.loglevel)

# Set up logging
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=num_level, format=log_format)
log = logging.getLogger(__name__)
log.debug('Loglevel is %s', logging.getLevelName(log.getEffectiveLevel()))

# Update default settings from the settings file
with open(args.config) as f:
    overrides = json.load(f)
    if 'backup' in overrides and isinstance(overrides['backup'], dict):
        settings['backup'].update(overrides['backup'])
    if 'cifs' in overrides and isinstance(overrides['cifs'], dict):
        settings['cifs'].update(overrides['cifs'])
    if 'mqtt' in overrides and isinstance(overrides['mqtt'], dict):
        settings['mqtt'].update(overrides['mqtt'])


#
# MQTT CONNECTION
#

namespace = settings['mqtt']['pub_topic_namespace']

# Set up paho-mqtt
log.debug("Initializing MQTT...")
mqtt_client = mqtt.Client(
    client_id = settings['mqtt']['client_id'])

if settings['mqtt']['username']:
    mqtt_client.username_pw_set(
        settings['mqtt']['username'],
        settings['mqtt']['password'])

# The will makes sure the device registers as offline when the connection
# is lost
mqtt_client.will_set(
    topic = namespace,
    payload = "offline",
    qos = settings['mqtt']['qos'],
    retain = True)

mqtt_client.connect_async(
    host = settings['mqtt']['host'],
    port = settings['mqtt']['port'],
    keepalive = settings['mqtt']['keepalive'],
    bind_address = settings['mqtt']['bind_address'])
mqtt_client.loop_start()

# First message telling that the backup is in progress
sleep(3) # give mqtt some time to start
mqtt_client.publish(
    topic = namespace,
    payload = "busy",
    qos = settings['mqtt']['qos'],
    retain = True)


#
# PREPARING FOR BACKUP
#

mountpoint = settings['cifs']['target']
mountcmd = 'mount -t cifs -o user={},password={},rw,file_mode=0777,dir_mode=0777 {} {}'.format(
    settings['cifs']['user'], 
    settings['cifs']['password'], 
    settings['cifs']['share'],
    mountpoint)
backup_path = mountpoint + '/' + settings['cifs']['subDir']

# Check if mount point exists
log.debug("Checking mount point ({})...".format(mountpoint))
if not os.path.isdir(mountpoint):
    fatal_error(
        message = 'Invalid mount point {}'.format(mountpoint),
        error_code = 1)

# Mount the cifs share
if not os.path.ismount(mountpoint):
    log.debug("Mounting {}...".format(mountpoint))
    call(mountcmd, shell=True)

# Check if the share is successfully mounted and the backup directory is present
log.debug("Checking mount point...")
if not os.path.ismount(mountpoint):
    fatal_error(
        message = 'Failed to mount backup volume {}'.format(mountpoint),
        error_code = 2)
elif not os.path.isdir(backup_path):
    fatal_error(
        message = 'Invalid backup directory {}'.format(backup_path),
        error_code = 3,
        unmount = mountpoint)
    
# Device directory on backup volume
maccmd = "cat /sys/class/net/$(ip route show default | awk '/default/ {print $5}')/address"
device_directory = '{}_{}'.format(
    socket.gethostname(), #device hostname
    check_output(maccmd, shell=True).decode('utf-8').replace(":","").strip() #mac address
)
backup_path +=  '/{}'.format(device_directory)

if not os.path.isdir(backup_path):
    log.info("Creating directory {}...".format(device_directory))
    os.mkdir(backup_path)
    if not os.path.isdir(backup_path):
        fatal_error(
            message = 'Failed to create {}'.format(backup_path),
            error_code = 4,
            unmount = mountpoint)

# Construct image name
image = settings['backup']['image_base_name']
interval = settings['backup']['full_backup_interval'].lower()[0]
now = datetime.datetime.now()
if interval == "d":
    image += '_{:04d}-{:02d}-{:02d}.img'.format(
        now.year, 
        now.month, 
        now.day)
elif interval == "w":
    image += '_{:04d}-wk{:02d}.img'.format(
        now.year, 
        datetime.date(now.year, now.month, now.day).isocalendar()[1])
elif interval == "m":
    image += '_{:04d}-{:02d}.img'.format(
        now.year,
        now.month)
elif interval == "y":
    image += '_{:04d}.img'.format(now.year)

# Check if image exists
if os.path.isfile('{}/{}'.format(backup_path, image)):
    log.info("Updating {}".format(image))
else:
    log.info("Full backup to {}".format(image))


#
# INVOKE BACKUP
#

backupcmd = '{} start -c {}/{}'.format(
    settings['backup']['command'],
    backup_path, 
    image)
return_code = call(backupcmd, shell=True)  

if return_code > 0:
    fatal_error(
        message = 'Backup stopped with exit code {}'.format(return_code),
        error_code = 5,
        unmount = mountpoint)

#
# FINALISE
#

# Unmount the backup volume
return_code = call('umount ' + mountpoint, shell=True)

if return_code > 0:
    fatal_error(
        message = 'Failed to unmount the backup volume',
        error_code = 6)

# Report successful backup
now = str(datetime.datetime.now())
mqtt_publish(
    topic = "last_success",
    payload = now)

sys.exit(0)


