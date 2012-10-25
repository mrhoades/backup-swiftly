import os
import logging
import json
# hack to allow import from parent dir
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0,parent_dir)
from utils.swift import swift
from structs.hpcloud_creds import hp_cloud_creds


# logger settings
logging.basicConfig(format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BACKUP_SWIFTLY")
logger.setLevel(logging.INFO)


# load rscript config files
configPath  = open("config.json").read()
config = json.loads(configPath)


# hpcs account credentials
creds = hp_cloud_creds(str(config['hpcloud_account_info']['tenant_name']),
                        str(config['hpcloud_account_info']['auth_url']),
                        str(config['hpcloud_account_info']['username']),
                        str(config['hpcloud_account_info']['password']))


# construct utility objects
swift = swift(creds)


# delete all test files located in the backup container
swift_items = swift.get_swift_item_list_using_pattern(str(config['backup_node_info']['swift_container_name']), None)

for item in swift_items:
    logger.info("Delete item from SWIFT: " + item)
    swift.delete_file_from_swift(config['backup_node_info']['swift_container_name'],item)


