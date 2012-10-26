import logging
import optparse
import os
import re
import sys
import datetime
import json
import random
from utils.swift import swift
from structs.hpcloud_creds import hp_cloud_creds
from utils.tar import tar
from utils.crypto import crypto


# parse args
parser = optparse.OptionParser()
parser.add_option('-c','--config',dest='config',action="store", help='Path to config file that provides HP Cloud Credentials')
(options, args) = parser.parse_args()
if not options.config:
    print "Incorrect number of arguments. Please provide path to config.json. See README for details or copy the provided sample config."
    parser.print_help()
    sys.exit(0)

# logger settings
logging.basicConfig(format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BACKUP_SWIFTLY")
logger.setLevel(logging.INFO)

# load rscript config files
configPath  = open(options.config).read()
config = json.loads(configPath)

# hpcs account credentials
creds = hp_cloud_creds(str(config['hpcloud_account_info']['tenant_name']),
                    str(config['hpcloud_account_info']['auth_url']),
                    str(config['hpcloud_account_info']['username']),
                    str(config['hpcloud_account_info']['password']))

swift_container_name    = str(config['backup_node_info']['swift_container_name'])

backup_dirs_to_swift     = str(config['backup_node_info']['backup_dir_to_swift']).split(",")

server_name    = str(config['backup_node_info']['server_name'])

##### backup scenario ######
# 1. create a compressed tar file
# 2. encrypt the file
# 3. upload the file to swift

# construct utility objects
tar     = tar()
crypto  = crypto()
swift   = swift(creds)

# generate a random identifier, dates, friendly name, to help with cleanup and visually identifying related backups
randID = str(random.randint(100,999)) + "-" + str(random.randint(10000,99999))

# iterate through
for backup_dir_path in backup_dirs_to_swift:

    #backup_dir_friendly_name  =  os.path.basename(re.sub("/$","", str(backup_dir_path))) # clean trailing slash
    backup_dir_friendly_name  =  os.path.basename(re.sub("/","-", str(backup_dir_path))) # use to identify backup path

    dateToday = datetime.datetime.today()
    date = dateToday.strftime("%Y-%m-%d")

    backup_filename = "backup." + date + "." + server_name + "-" +randID + "-" + backup_dir_friendly_name
    backup_filename_tar_gz = backup_filename + ".tar.gz"
    backup_filename_encrypted = backup_filename_tar_gz + ".encrypted"

    logger.info("BACKUP DIR: " + backup_dir_path)
    logger.info("BACKUP TAR FILENAME: " + backup_filename)

    tar.create(backup_dir_path, backup_filename)

    crypto.encrypt_file(creds.password, backup_filename_tar_gz, backup_filename_encrypted)

    swift.upload(swift_container_name, backup_filename_encrypted)

