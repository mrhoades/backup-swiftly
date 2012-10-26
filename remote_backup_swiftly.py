import sys
import datetime
from utils.tar import tar
from utils.ssh_remote import ssh_remote_client
from structs.ssh_connection_info import ssh_connection_info
import logging
import os
import optparse


# parse args
parser = optparse.OptionParser()
parser.add_option("-s","--servername",dest="hostname", action="store", help="Hostname or IP where backup script will be executed.")
parser.add_option('-u','--username',dest='username',action="store", help='User name to connect with. Default is user ubuntu.', default="ubuntu")
parser.add_option('-d','--dropdir',dest='dropdir',action="store", help='Spacious directory where backup packaging will take place. The default is /mnt/backup.',default="/mnt/backup")
parser.add_option('-i','--identity',dest='identity_file',action="store", help='Identity file to use with paramiko ssh connect.',default=None)
parser.add_option('-c','--config',dest='remote_config',action="store", help='Path to config file on remote server. You may choose to store your cloud cred config file on the remote server.',default="./config.json")
parser.add_option('-k','--cleanup',dest='cleanup',action="store", help='Boolean value. When debugging, you may want to disable cleanup so you can inspect files. Default is True.',default=True)
(options, args) = parser.parse_args()
if not options.hostname:
    print "ERROR --servername (-s) server or ip is required input."
    parser.print_help()
    sys.exit(0)


# logging configuration
logging.basicConfig(format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BACKUP_SWIFTLY")
logger.setLevel(logging.DEBUG)


# ssh connection info and creds
sshinfo = ssh_connection_info(options.hostname, options.username, None, options.identity_file)


# generate unique backup and drop dirs
date = datetime.datetime.today()
date = date.strftime("%Y-%m-%d")
backup_drop_dir = str(options.dropdir)
backup_dir = backup_drop_dir + "/backup_" + date


# construct utility objects
tar = tar()
ssh = ssh_remote_client(sshinfo)


# create script package - tar up all script files
script_package_tar_gz = tar.create(os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir)),"backup_swiftly")


# force create backup dir (soft fails if it already exists)
ssh.run_command("sudo mkdir " + backup_drop_dir)
ssh.run_command("sudo mkdir " + backup_dir)


# put script package on remote host - drop to default home dir
ssh.put_file(script_package_tar_gz, script_package_tar_gz)


# unpack and execute script
ssh.run_command("sudo tar -C " + backup_dir + " -zxvf " + script_package_tar_gz)
ssh.run_command("sudo chmod -R +x " + backup_dir)
ssh.run_command("sudo rm -rf " + script_package_tar_gz)
ssh.run_command("sudo apt-get install python")
ssh.run_command("sudo apt-get install python-paramiko")
ssh.run_command("cd " + backup_dir + " \n sudo python ./backup_to_swift.py -c " + options.remote_config)


# scrub the script and backup files from the system (no file left behind)
if options.cleanup:
    ssh.run_command("sudo rm -rf " + backup_dir)
