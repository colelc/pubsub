import paramiko
from src.config.config import Config
from src.logging.app_logger import AppLogger

class ParamikoClient(object):

    def __init__(self, host):
        self.logger = AppLogger.get_logger()

        self.user = Config.get_property("smtp.user.name")
        self.key_filename = Config.get_property("smtp.private.key.file")

        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.client.connect(hostname=host, username=self.user, key_filename=self.key_filename)
        except Exception as e:
            self.logger.error(str(e))

    def get_paramiko_client(self):
        return self.client
    
    def close_paramiko_client(self):
        self.get_paramiko_client().close()

