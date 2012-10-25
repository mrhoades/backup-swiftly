from utils.swift import swift
from utils.tar import tar
from utils.ssh_remote import ssh_remote_client
from structs.hpcloud_creds import hp_cloud_creds
from structs.ssh_connection_info import ssh_connection_info

import logging
import socket
import datetime
import json
import os
import optparse

host = socket.gethostname()
date = datetime.datetime.today()
date = date.strftime("%Y-%m-%d")

# parse args
parser = optparse.OptionParser()
parser.add_option('-u','--username',dest='username',action="store", help='User name to connect with. Default is user ubuntu.', default="ubuntu")
parser.add_option('-d','--dropdir',dest='drop_dir',action="store", help='Spacious directory where backup packaging will take place. The default is /mnt/backup.',default="/mnt/backup")

(options, args) = parser.parse_args()

#### logging settings ####
logging.basicConfig(format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BACKUP_SWIFTLY")
logger.setLevel(logging.DEBUG)


#### load config ####
configFile  = open("config.json").read()
config      = json.loads(configFile)


#### hpcs account credentials ####
creds = hp_cloud_creds(str(config['hpcloud_account_info']['tenant_name']),
                       str(config['hpcloud_account_info']['auth_url']),
                       str(config['hpcloud_account_info']['username']),
                       str(config['hpcloud_account_info']['password']))


#### ssh connection info and creds ####
sshinfo = ssh_connection_info(str(config['backup_node_info']['server_name']),options.username)

backup_dir = options.drop_dir + "_" + date

#### construct utility objects
tar     = tar()
swift   = swift(creds)
ssh     = ssh_remote_client(sshinfo)


# create script package
path_to_script_files = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
script_package_tar_gz = tar.create(path_to_script_files,"backup_swiftly")

# force create backup dir (soft fails if it already exists)
ssh.run_command("sudo mkdir " + backup_dir)

# put script package on remote host - drop to default home dir (to avoid permission issues)
ssh.put_file(script_package_tar_gz,script_package_tar_gz)

# unpack and execute script
ssh.run_command("sudo tar -C " +backup_dir+ " -zxvf " + script_package_tar_gz)
ssh.run_command("sudo apt-get install python")
ssh.run_command("sudo apt-get install python-paramiko")
ssh.run_command("sudo chmod -R +x " +backup_dir)
ssh.run_command("cd " + backup_dir + "/tests/ \n sudo python ./test_backup_swiftly.py ./config.json")

# cleanup goobers
ssh.run_command("sudo rm -rf " + backup_dir)


