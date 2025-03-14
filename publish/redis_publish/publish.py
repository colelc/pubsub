from src.logging.app_logger import AppLogger
from src.services.redis_publish import RedisPublish

class Publish(object):

    @classmethod
    def go(cls):
        log = AppLogger.get_logger()
        log.info("")
        log.info("This is code to publish a message to a redis channel")
        log.info("")

        #RedisPublish().redis_publish()
        RedisPublish().redis_publish_to_stream()

        log.info("DONE")

Publish.go()