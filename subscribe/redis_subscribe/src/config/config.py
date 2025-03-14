import os 
import socket
import sys
from dotenv import dotenv_values
from src.logging.app_logger import AppLogger
from pathlib import Path

class Config(object):
    config = None
    ENV = "d" # default to development environment
   
    @staticmethod
    def set_up_config() -> dict:
        logger = AppLogger.get_logger()
        
        try:
            host_name = socket.gethostname()
            host_ip = socket.gethostbyname(host_name)
        except Exception as e:
            logger.error(str(e))
            sys.exit(99)

        #for key, value in os.environ.items():
        #    logger.info(key + " -> " + str(value))
        full_config = dotenv_values(os.path.join(os.getenv("PYTHONPATH"), "resources", ".env"))
        Config.ENV = full_config.get("ENV")

        filtered_by_environment = dict(filter(lambda key: key[0].startswith(Config.ENV+"."), full_config.items()))
        config = dict(map(lambda kv: (kv[0][2:], kv[1]), filtered_by_environment.items()))
 
        logger.info("CONFIGURATION BEGIN : ******************************************************")
        logger.info("These are the configuration values")
        for key,value in config.items():
            logger.info("CONFIGURATION: " + key + " -> " + value)
        logger.info("CONFIGURATION END   : ******************************************************")
        logger.info("ENVIRONMENT: The machine host name and IP is: " + host_ip + " " + host_name)
        
        return config
    
    @staticmethod
    def get_ENV() -> str:
        if ENV == None:
            ENV = Config.get_config_value("ENV")
        return ENV

    """
    @staticmethod
    def get_config_value(key: str) -> str:
        # this is crazy - there must be a better way to look up an entry
        keys = list(Config.get_config().keys())

        try:
            ix = keys.index(key)
            return list(Config.get_config().items())[ix][1]
        except ValueError as ve:
            logger = AppLogger.get_logger()
            logger.error("Cannot find config value for key: " + key)
            return None
    """

    @staticmethod
    def get_property(key:str) -> str:
        return Config.get_config().get(key)
    
    @staticmethod
    def get_config():
        if Config.config is None:
            Config.config = Config.set_up_config()
            
        return Config.config
    
#Config.get_config()