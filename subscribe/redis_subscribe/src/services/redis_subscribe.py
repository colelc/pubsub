import subprocess
import os
import random
import redis
import sys
import time
from redis.cluster import RedisCluster, ClusterNode
from src.config.config import Config
from src.logging.app_logger import AppLogger
from src.services.message_processor import MessageProcessor


class RedisSubscribe(object):

    def __init__(self):
        self.logger = AppLogger.get_logger()
        self.redis_client = self.set_up_client()

        self.list_work_directory = Config.get_property("list.work.directory")
        self.list_staging_directory = Config.get_property("list.staging.directory")
   
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

    def get_redis_client(self):
        return self.redis_client

    def redis_listen_stream(self):
        self.logger.info("Listening for messages from the redis stream ....")

        stream_name = Config.get_property("redis.stream")
        consumer_group = Config.get_property("consumer.group")
        consumer_name = Config.get_property("consumer.name")

        while True:
            try:
                entries = self.get_redis_client().xreadgroup(consumer_group, consumer_name, {stream_name: ">"}, count=1, block=5000)
                if entries:
                    for entry_list in entries: 
                        #self.logger.info("entry_list: " + str(type(entry_list)) + " -> " +  str(entry_list))
                        #stream_name = entry_list[0]

                        data_list = entry_list[1]
                        #self.logger.info("data_list: " + str(type(data_list)) + " -> " + str(data_list))
                        for data in data_list:
                            #self.logger.info(str(type(data)) + " -> " + str(data))
                            timestamp_marker = data[0].decode("utf-8")
                            dct = data[1]
                            dn = dct[b'dn'].decode("utf-8")
                            #self.logger.info(timestamp_marker + " -> " + str(dn))

                            # return_code, job_file_path = self.process_message(timestamp_marker, dn)
                            return_code, job_file_path = MessageProcessor(timestamp_marker, dn).process_message()

                            if return_code == 0:
                                # acknowledge ensures the message will not get delivered again
                                acknowledged_count = self.get_redis_client().xack(stream_name, consumer_group, timestamp_marker)
                                if acknowledged_count == 1:
                                    self.logger.info(str(acknowledged_count) + " acknowledgement issued for " + timestamp_marker + " " + dn)
                                else:
                                    self.logger.error("Acknowledged message count should be 1, but is " + str(acknowledged_count) + " for:")
                                    self.logger.error("timestamp marker: " + str(timestamp_marker) + " dn=" + str(dn))
                            else:
                                self.logger.error("return_code from publish_list.py is " + str(return_code) + ": see " + job_file_path)
                            
            except redis.exceptions.ConnectionError as econn:
                self.logger.error(str(econn))
                self.logger.error("Redis connection lost.")
                time.sleep(60)
            except redis.exceptions.BusyLoadingError as eload:
                self.logger.error(str(eload))
                time.sleep(60)
            except KeyboardInterrupt:
                self.logger.info("Exiting by user request")
                break
            except Exception as e:
                self.logger.error(str(e))
                time.sleep(60)
                