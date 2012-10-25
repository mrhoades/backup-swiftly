import paramiko
import logging

class ssh_remote_client:

    def __init__(self, SSHConnectionInfo):

        self.logger = logging.getLogger("BACKUP_SWIFTLY")
        self.logger.setLevel(logging.DEBUG)

        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.logger.info("SSH Connect to remote host at: " + SSHConnectionInfo.server_name_or_ip)
        self.client.connect(SSHConnectionInfo.server_name_or_ip,
                            SSHConnectionInfo.port,
                            SSHConnectionInfo.username,
                            SSHConnectionInfo.password,
                            None,None,5,
                            allow_agent=True,
                            look_for_keys=True)

    def put_file(self,local_path_to_file,remote_path_to_file):
        self.logger.debug("SFTP file %s to remote host",remote_path_to_file)
        self.sftp=self.client.open_sftp()
        self.sftp.put(local_path_to_file,remote_path_to_file,callback=self.print_progress,confirm=True)

    def print_progress(self,transferred, toBeTransferred):
        self.logger.debug("Transferred: {0}\tStill to send: {1}".format(transferred, toBeTransferred))

    def run_command(self,str_command):
        self.logger.info('Run command \'' +str_command + '\'on remote machine')
        stdin, stdout, stderr = self.client.exec_command(str_command)

        for i, line in enumerate(stdout):
            line = line.rstrip()
            self.logger.info("%d: %s" % (i, line))

        for i, line in enumerate(stderr):
            line = line.rstrip()
            self.logger.info("%d: %s" % (i, line))

