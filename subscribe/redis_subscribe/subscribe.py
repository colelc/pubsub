from src.logging.app_logger import AppLogger
from src.services.redis_subscribe import RedisSubscribe

class Subscribe(object):

    @classmethod
    def go(cls):
        log = AppLogger.get_logger()
        log.info("")
        log.info("This is code to subscribe to a redis stream")
        log.info("")

        #RedisSubscribe().redis_listen()
        RedisSubscribe().redis_listen_stream()

        log.info("DONE")

Subscribe.go()