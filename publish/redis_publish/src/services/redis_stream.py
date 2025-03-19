import sys
from redis.cluster import RedisCluster, ClusterNode
from src.config.config import Config
from src.logging.app_logger import AppLogger


class RedisStream(object):

    def __init__(self):
        self.logger = AppLogger.get_logger()
        self.redis_client = self.set_up_client()
        self.channel = Config.get_property("redis.channel")
        self.set_up_consumer_group()
   
    def set_up_client(self) -> type:
        # specifying all 6 redis nodes in the cluster, but could specify any single node: that node would self-discover the rest of the cluster
        startup_nodes = [
                        {"host": Config.get_property("local.host"),  "port": Config.get_property("port.1")},
                        {"host": Config.get_property("local.host"),  "port": Config.get_property("port.2")},
                        {"host": Config.get_property("local.host"),  "port": Config.get_property("port.3")},
                        {"host": Config.get_property("remote.host"),  "port": Config.get_property("port.1")},
                        {"host": Config.get_property("remote.host"),  "port": Config.get_property("port.2")},
                        {"host": Config.get_property("remote.host"),  "port": Config.get_property("port.3")}
                        ]
    
        kwargs = dict()
        kwargs["startup_nodes"] = [ClusterNode(**node) for node in startup_nodes]
        kwargs["decodeResponses"] = True
        kwargs["username"] = Config.get_property("redis.user")
        kwargs["password"] = Config.get_property("redis.password")
        
        try:
            # Connect to the redis cluster
            redis_client = RedisCluster(**kwargs)
        except Exception as e:
            self.logger.error(str(e))
            sys.exit(99)

        return redis_client
    
    def set_up_consumer_group(self):
        stream_name = Config.get_property("redis.stream")
        consumer_group = Config.get_property("consumer.group")
        self.logger.info("Setting up stream: " + stream_name + " for consumer group: " + consumer_group)

        try:
            self.get_redis_client().xgroup_create(stream_name, consumer_group, id="0", mkstream=True)
        except Exception as e:
            self.logger.error(str(e))
            #sys.exit(99)

    def get_redis_client(self):
        return self.redis_client

    
