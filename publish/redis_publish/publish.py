from src.config.config import Config
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
        #RedisPublish().redis_publish_to_stream()

        # here, instantiate RedisPublish: this will ensure the redis stream is created
        redis_publish = RedisPublish()

        # if we need our client to inject test messages to the redis stream
        env = Config.get_property("environment")
        if env != "production":
            log.info("Injecting test message into the redis stream...")
            redis_publish.redis_publish_to_stream()

        log.info("DONE")

Publish.go()