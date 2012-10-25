import os
import re
import socket
import datetime
# hack to allow import from parent dir
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0,parent_dir)
from utils.compare import compare

# run backup script
os.system("python ../backup_to_swift.py -c config.json")

# get swift package, decrypt, and unpack it
os.system("python ../download_decrypt_and_upack_swift_files.py -n test-backups -f 'test_backup_files' -c 'config.json' -d ./decrypted_swift_files/")

# compare extracted fileset to original fileset
comp = compare()
if not comp.are_dirs_equal('./decrypted_swift_files/test_backup_files','./test_backup_files'):
    raise Exception('FAIL: Files downloaded from swift, decrypted, and unpacked, do not match what was uploaded.')

# cleanup test goobers from swift
os.system("python ./cleanup_test_files.py -c config.json")

# cleanup local test goobers
host = socket.gethostname()
date = datetime.datetime.today()
date = date.strftime("%Y-%m-%d")
cleanup_pattern = "backup." + date + "." + host
for f in os.listdir(os.path.dirname(__file__)):
    if re.search(cleanup_pattern, f):
        os.remove(os.path.join(os.path.dirname(__file__), f))

os.system("rm -rf ./decrypted_swift_files")

print "Script Complete!"