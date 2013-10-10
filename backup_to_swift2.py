import logging
import optparse
import os
import re
import sys
import datetime
import random
from utils.swift import swift
from structs.hpcloud_creds import hp_cloud_creds
from utils.tar import tar
from utils.crypto import crypto


# parse args
parser = optparse.OptionParser()
parser.add_option("-n", "--name",
                  dest="backup_preamble",
                  action="store",
                  help="Filename Preamble to help identify packages in swift. For example 'PAAS-JENKINS'",
                  default='PREAMBLE-NAME')
parser.add_option("-c", "--container-name",
                  dest="container_name",
                  action="store",
                  help="Name of the swift container to store the backup in. Will be created if it doesn't exist.",
                  default='paas-test-backups')
parser.add_option("-f", "--file-path",
                  dest="file_paths_to_backup",
                  action="store",
                  help="Comma separated list of directory paths, that will be tar'd, encrypted, and pushed to swift.")
parser.add_option("-i", "--ignore-path",
                  dest="dir_paths_to_ignore",
                  action="store",
                  help="Comma separated list of directory paths that should be ignored.",
                  default="")
parser.add_option("-j", "--ignore-file",
                  dest="file_paths_to_ignore",
                  action="store",
                  help="Comma separated list of file paths that should be ignored.",
                  default="")
parser.add_option("-u", "--username",
                  dest="username",
                  action="store",
                  help="HP Cloud account username",
                  default='rocketboy')
parser.add_option("-p", "--password",
                  dest="password",
                  action="store",
                  help="HP Cloud password")
parser.add_option("-t", "--tenant-name",
                  dest="tenant_name",
                  action="store",
                  help="HP Cloud tenant name",
                  default='mrhoades@hp.com-tenant1')
parser.add_option("-a", "--auth-url",
                  dest="auth_url",
                  action="store",
                  help='HP Cloud Auth URL.',
                  default="https://region-a.geo-1.identity.hpcloudsvc.com:35357/v2.0/")

(options, args) = parser.parse_args()
if not options.password:
    print "ERROR please provide credential information for for your cloud account and details about the backup."
    # bugbugbug - need to beef up print usage and error checking here
    parser.print_help()
    sys.exit(0)


# logger settings
logging.basicConfig(format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BACKUP_SWIFTLY")
logger.setLevel(logging.INFO)


# hpcs account credentials
creds = hp_cloud_creds(options.tenant_name, options.auth_url, options.username, options.password)


##### backup scenario ######
# 1. create a compressed tar file
# 2. encrypt the file
# 3. upload the file to swift


# construct utility objects
tar = tar()
crypto = crypto()
swift = swift(creds)


# generate a random identifier, dates, friendly name, to help with cleanup and visually identifying related backups
random_id = str(random.randint(100, 999)) + "-" + str(random.randint(10000, 99999))


# iterate through provided dirs to backup
backup_dirs_to_swift = options.file_paths_to_backup.split(",")
for backup_dir_path in backup_dirs_to_swift:

    dateToday = datetime.datetime.today()
    date = dateToday.strftime("%Y-%m-%d")

    normalized_path = os.path.abspath(backup_dir_path)
    backup_dir_friendly_name = os.path.basename(re.sub("/", "-", str(normalized_path)))
    backup_filename = options.backup_preamble + "--" + date + "--RID-" + random_id + "--" + backup_dir_friendly_name
    backup_filename_tar_gz = backup_filename + ".tar.gz"
    backup_filename_encrypted = backup_filename_tar_gz + ".encrypted"

    logger.info("BACKUP DIR: " + backup_dir_path)
    logger.info("BACKUP TAR FILENAME: " + backup_filename)

    tar.create(backup_dir_path, backup_filename, options.dir_paths_to_ignore, options.file_paths_to_backup)

    crypto.encrypt_file(creds.password, backup_filename_tar_gz, backup_filename_encrypted)

    swift.upload(options.container_name, backup_filename_encrypted)

