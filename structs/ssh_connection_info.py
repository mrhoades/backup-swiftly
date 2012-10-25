
class ssh_connection_info:
    def __init__(self,server_name_or_ip,username,password=None,path_to_identify_file=None,port=22):
        self.server_name_or_ip = server_name_or_ip
        self.username = username
        self.password = password
        self.identity_file_path = path_to_identify_file
        self.port = port
    def display(self):
        print self.server_name_or_ip
        print self.username
        print self.password
        print self.identity_file_path
        print self.port
