import logging
import optparse
import sys
import json
from utils.swift import swift
from structs.hpcloud_creds import hp_cloud_creds


# parse args
parser = optparse.OptionParser()
parser.add_option('-n','--name',dest='swift_container_name',action="store", help='Name if swift container to list',default=True)
parser.add_option('-c','--config',dest='remote_config',action="store", help='Path to config file that has hpcloud swift credentials',default="./config.json")
(options, args) = parser.parse_args()

if not options.swift_container_name:
    print "ERROR --name (-n) swift container name is required to list container items!"
    parser.print_help()
    sys.exit(0)

# logger settings
logging.basicConfig(format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BACKUP_SWIFTLY")
logger.setLevel(logging.INFO)


# load rscript config files
configPath  = open(options.remote_config).read()
config = json.loads(configPath)


# hpcs account credentials
creds = hp_cloud_creds(str(config['hpcloud_account_info']['tenant_name']),
                    str(config['hpcloud_account_info']['auth_url']),
                    str(config['hpcloud_account_info']['username']),
                    str(config['hpcloud_account_info']['password']))


# construct utility objects
swift   = swift(creds)


swift.list_container_items(options.swift_container_name)

