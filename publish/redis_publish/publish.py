from src.config.config import Config
from src.logging.app_logger import AppLogger
from src.services.redis_client import RedisClient
from src.services.redis_publish import RedisPublish
from src.services.redis_stream_creator import RedisStreamCreator

class Publish(object):

    @classmethod
    def go(cls):
        log = AppLogger.get_logger()
        log.info("")
        log.info("This is code to create a redis stream and inject a test message if not running in production...")
        log.info("")

        redis = RedisClient()
        #redis_client = RedisClient().get_redis_client()

        RedisStreamCreator(redis.get_redis_client())

        # if we need our client to inject test messages to the redis stream
        env = Config.get_property("environment")
        if env != "production":
            log.info("Injecting test message into the redis stream...")
            RedisPublish().redis_publish_to_stream(redis.get_redis_client())

        redis.close_redis_client()

        log.info("DONE")

Publish.go()