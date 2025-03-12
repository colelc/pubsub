#from src.config.config import Config
from src.logging.app_logger import AppLogger
from src.services.beanstalk_subscribe import BeanstalkSubscribe
#import greenstalk
import sys

## https://www.perplexity.ai/search/how-can-i-write-a-python-progr-T.BI3UbIQ_mW2V5XRp34xQ
## https://greenstalk.readthedocs.io/en/stable/api.html
class Subscribe(object):

    @classmethod
    def go(cls):
        log = AppLogger.get_logger()
        log.info("")
        log.info("This is code to watch a beanstalk queue")
        log.info("")

        #config = Config.get_config()

        BeanstalkSubscribe().beanstalk_watcher()

        log.info("DONE")

Subscribe.go()