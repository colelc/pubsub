import random
import time
from src.config.config import Config
from src.logging.app_logger import AppLogger

class RedisPublish(object):

    def __init__(self):
        self.logger = AppLogger.get_logger()
           
    def redis_publish_to_stream(self, redis_client):
        test_message = Config.get_property("test.message")

        stream_name = Config.get_property("redis.stream")

        iteration_count = 0
        iterations = int(Config.get_property("publish.iterations"))

        while iteration_count < iterations:
            iteration_count += 1
            wait_time = random.randint(1, 5)
            self.logger.info("Waiting " + str(wait_time) + " seconds") 
            time.sleep(wait_time)

            try:
                redis_client.xadd(stream_name, {"dn": test_message})
            except Exception as e:
                self.logger.error(str(e))
    