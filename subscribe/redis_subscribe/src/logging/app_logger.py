import logging
import os
import sys

class AppLogger(object):
    logger = None

    @staticmethod
    def set_up_logger():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        #PROJECT_HOME = os.getenv("PYTHONPATH") # set in myvenv/bin/activate
        log_file_name = os.path.join(os.getenv("PYTHONPATH"), "major_update.log") # set in .venv/bin/activate
        
        file_handler = logging.FileHandler(log_file_name)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(filename)s %(funcName)s %(lineno)s: %(message)s")
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger
    
    @staticmethod
    def get_logger():
        if AppLogger.logger is None:
            AppLogger.logger = AppLogger.set_up_logger()
            
        return AppLogger.logger
    
#AppLogger.get_logger().info("hello")