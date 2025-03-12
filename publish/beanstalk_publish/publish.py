from src.config.config import Config
from src.logging.app_logger import AppLogger
from src.services.beanstalk_publish import BeanstalkPublish
import greenstalk

## https://www.perplexity.ai/search/i-have-a-python-program-that-u-9lpvUfcqQKi0Y2wEo3z2.w
## https://greenstalk.readthedocs.io/en/stable/api.html
class Publish(object):

    @classmethod
    def go(cls):
        log = AppLogger.get_logger()
        log.info("")
        log.info("This is code to publish a message to a beanstalk queue")
        log.info("")

        config = Config.get_config()

        beanstalk_client = BeanstalkPublish.get_beanstalk_client()
        tubes = beanstalk_client.watch(Config.get_property("tube"))

        BeanstalkPublish.beanstalk_publish()



        log.info("DONE")

Publish.go()