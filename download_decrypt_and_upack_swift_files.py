import logging
import optparse
import re
import sys
from utils.swift import swift
from structs.hpcloud_creds import hp_cloud_creds
from utils.tar import tar
from utils.crypto import crypto
import json


# parse args
parser = optparse.OptionParser()
parser.add_option('-n','--container_name',dest='swift_container_name',action="store", help='Name of swift container to look for files in.')
parser.add_option('-f','--filename',dest='swift_file_name_or_pattern',action="store", help='Name of file in swift or pattern that should match multiple files.')
parser.add_option('-c','--config',dest='config',action="store", help='Path to config file that has hpcloud swift credentials',default="./config.json")
parser.add_option('-d','--drop_dir',dest='drop_dir',action="store", help='Path where swift files should be unpacked to.',default="./decrypted_swift_files/")
(options, args) = parser.parse_args()
if not options.swift_container_name and options.swift_file_name_or_pattern:
    print "ERROR (-n) swift container name and (-f) file name/match pattern are required."
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


# construct utility objects
tar = tar()
crypto = crypto()
swift = swift(creds)

swift_items = swift.get_swift_item_list_using_pattern(options.swift_container_name, options.swift_file_name_or_pattern)

for item_name in swift_items:

    swift.download(options.swift_container_name, item_name, item_name)

    cleaned_name = re.sub(".encrypted$","", item_name)

    crypto.decrypt_file(creds.password, item_name, cleaned_name)

    tar.extract(options.drop_dir,item_name)

