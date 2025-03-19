import random
import sys
import time
from redis.cluster import RedisCluster, ClusterNode
from src.config.config import Config
from src.logging.app_logger import AppLogger
from src.services.redis_stream import RedisStream


class RedisPublish(object):

    def __init__(self):
        self.logger = AppLogger.get_logger()
        self.redis_client = RedisStream().get_redis_client()
           
    def redis_publish_to_stream(self):
        test_message = Config.get_property("test.message")
        #self.logger.info(test_message)

        stream_name = Config.get_property("redis.stream")

        iteration_count = 0
        iterations = int(Config.get_property("publish.iterations"))

        while iteration_count < iterations:
            iteration_count += 1
            wait_time = random.randint(1, 5)
            self.logger.info("Waiting " + str(wait_time) + " seconds") 
            time.sleep(wait_time)
            #self.logger.info("Publishing to redis stream ...")

            try:
                #response = self.get_redis_client().publish(self.channel, test_message)
                #self.logger.info("Message published to redis cluster...")
                #self.logger.info(str(response))
                self.redis_client.xadd(stream_name, {"dn": test_message})
            except Exception as e:
                self.logger.error(str(e))
    
    def redis_publish(self):
        test_message = Config.get_property("test.message")
        #self.logger.info(test_message)

        iteration_count = 0
        iterations = int(Config.get_property("publish.iterations"))

        while iteration_count < iterations:
            iteration_count += 1
            wait_time = random.randint(1, 5)
            self.logger.info("Waiting " + str(wait_time) + " seconds") 
            time.sleep(wait_time)
            self.logger.info("Publishing to redis ...")

            try:
                response = self.redis_client.publish(self.channel, test_message)
                self.logger.info("Message published to redis cluster...")
                self.logger.info(str(response))
            except Exception as e:
                self.logger.error(str(e))


    
