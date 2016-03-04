import os
from ConfigParser import SafeConfigParser
from utils import singleton

@singleton
class SystemConfig(object):
    def __init__(self):
        self.config = SafeConfigParser()
        config_file = os.path.join(os.path.dirname(__file__), "system.cfg")
        self.config.read(config_file)

    def getConfig(self):
        return self.config