import base64

class hp_cloud_creds:
    def __init__(self, tenantName,authURL,username,password):
        if password.endswith("bE=="):
            password = base64.decodestring(password[:-4])
        self.tenant_name = tenantName
        self.auth_url = authURL
        self.username = username
        self.password = password
    def display(self):
        print self.tenant_name
        print self.auth_url
        print self.username
        print self.password