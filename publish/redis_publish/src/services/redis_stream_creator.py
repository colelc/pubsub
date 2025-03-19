from src.config.config import Config
from src.logging.app_logger import AppLogger


class RedisStreamCreator(object):

    def __init__(self, redis_client):
        self.logger = AppLogger.get_logger()
        self.set_up_consumer_group(redis_client)
    
    def set_up_consumer_group(self, redis_client):
        stream_name = Config.get_property("redis.stream")
        consumer_group = Config.get_property("consumer.group")
        self.logger.info("Setting up stream: " + stream_name + " for consumer group: " + consumer_group)

        try:
            redis_client.xgroup_create(stream_name, consumer_group, id="0", mkstream=True)
        except Exception as e:
            # if group already exists, just ignore error message
            self.logger.error(str(e))
            #sys.exit(99)


    
