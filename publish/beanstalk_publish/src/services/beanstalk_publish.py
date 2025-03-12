import random
import sys
import time
import greenstalk
from src.config.config import Config
from src.logging.app_logger import AppLogger


class BeanstalkPublish(object):
    beanstalk_client = None
   
    @staticmethod
    def set_up_client() -> type:
        logger = AppLogger.get_logger()

        beanstalk_server = Config.get_property("beanstalk.server")
        beanstalk_port = Config.get_property("beanstalk.port")
        
        try:
            # Connect to the Beanstalk server
            # by specifying a tube value for use and watch, the default tube is ignored
            tube = Config.get_property("tube")
            beanstalk_client = greenstalk.Client((beanstalk_server, beanstalk_port), use=tube, watch=tube)
            logger.info("beanstalk client: " + str(beanstalk_client) + " tube: " + tube)
        except Exception as e:
            logger.error(str(e))
            sys.exit(99)

        return beanstalk_client

    @staticmethod
    def get_beanstalk_client():
        if BeanstalkPublish.beanstalk_client is None:
            BeanstalkPublish.beanstalk_client = BeanstalkPublish.set_up_client()
            
        return BeanstalkPublish.beanstalk_client
    
    @staticmethod
    def beanstalk_publish():
        log = AppLogger.get_logger()

        test_message = Config.get_property("test.message")
        #log.info(test_message)

        iteration_count = 0
        iterations = int(Config.get_property("publish.iterations"))

        while iteration_count < iterations:
            iteration_count += 1
            wait_time = random.randint(1, 5)
            log.info("Waiting " + str(wait_time) + " seconds") 
            time.sleep(wait_time)
            log.info("Publishing to beanstalk queue ...")

            try:
                BeanstalkPublish.get_beanstalk_client().put(test_message)
                #BeanstalkPublish.get_beanstalk_client().put("TEST " + str(iteration_count))
            except Exception as e:
                log.error(str(e))


    
