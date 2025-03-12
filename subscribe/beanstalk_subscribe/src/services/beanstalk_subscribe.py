import os
import signal
import subprocess
import sys
import time
import greenstalk
from src.config.config import Config
from src.logging.app_logger import AppLogger


class BeanstalkSubscribe(object):
    def __init__(self):
        self.logger = AppLogger.get_logger()
        self.beanstalk_client = self.set_up_client()

        signal.signal(signal.SIGINT, self.interrupt_handler)
        signal.signal(signal.SIGTERM, self.terminate_handler)

        self.list_work_directory = Config.get_property("list.work.directory")
        self.list_staging_directory = Config.get_property("list.staging.directory")
        
    def set_up_client(self) -> type:
        beanstalk_server = Config.get_property("beanstalk.server")
        beanstalk_port = Config.get_property("beanstalk.port")
        
        try:
            # Connect to the Beanstalk server
            # by specifying a tube value for use and watch, the default tube is ignored
            tube = Config.get_property("tube")
            beanstalk_client = greenstalk.Client((beanstalk_server, beanstalk_port), use=tube, watch=tube)

            watched_tubes = beanstalk_client.watching()
            if len(watched_tubes) != 1:
                self.logger.error("Watching more than 1 tube: this is not allowed.  Exiting.")
                sys.exit()

            if watched_tubes[0] != Config.get_property("tube"):
                self.logger.error("Watching tube: " + watched_tubes[0] + ". Should be watching tube: " + Config.get_property("tube") + ". Exiting.")
                sys.exit()

            self.logger.info("beanstalk client: " + str(beanstalk_client) + " tube: " + tube)
        except Exception as e:
            self.logger.error(str(e))
            sys.exit(99)

        return beanstalk_client

    def get_beanstalk_client(self):
        return self.beanstalk_client
    
    def beanstalk_watcher(self):
        self.logger.info("Waiting for jobs...")
        while True:
            try:
                # Reserve a job from the queue
                job = self.get_beanstalk_client().reserve()
                self.logger.info("A job is reserved from the queue")

                # Request job info
                dct = self.get_beanstalk_client().stats_job(job.id)
                self.logger.info("JOB INFO: " + str(dct))
                #for k,v in dct.items():
                #    self.logger.info(k + " -> " + str(v))

                # get bury priority value
                bury_priority = int(dct.get("pri"))

                # Process the job
                self.process_job(job)
                
                # Delete the job after processing
                self.get_beanstalk_client().delete(job)
            except greenstalk.TimedOutError:
                self.logger.info("No jobs available, waiting...")
            except Exception as e:
                self.logger.error(f"Error processing job: {e}")
                # You might want to bury or release the job here
                self.get_beanstalk_client().bury(job)
    
    def process_job(self, job):
        self.logger.info("Processing job: " + str(job))
        time.sleep(5)
        # here, we want to create the marker file, /tmp/job-[job.id].out
        # Stdout and Stderr stuff is written to marker file.
        # If the below command is successful, then the marker file is deleted.
        # /bin/sh -cex doIt.sh gid=1335536055293,ou=StaffDisplayGroups,ou=Departments,o=Fuqua,c=US
        #

        job_file_dir = Config.get_property("job.file.directory")
        job_file_name = "job-" + str(job.id) + ".out"
        job_file_path = os.path.join(job_file_dir, job_file_name)
        self.logger.info("Preparing job marker file: " + job_file_path)

        bash_script_dir = os.getenv("PYTHONPATH")
        bash_script_name = Config.get_property("job.script.name")
        bash_script_path = os.path.join(bash_script_dir, bash_script_name)

        with open(job_file_path, "w") as job_file:
            self.logger.info("Calling " + str(bash_script_name) + " with job.body=" + str(job.body))
            # result = subprocess.run(["/bin/sh", "-cex", bash_script_path, "", str(job.body)], capture_output=True, text=True, timeout=60)
            # result = subprocess.run(["/bin/sh", "-cex", bash_script_path, str(job.body)], capture_output=True, text=True, timeout=60)
            result = subprocess.run(["/bin/sh", "-e", "-x", bash_script_path, str(job.body), self.list_work_directory, self.list_staging_directory], capture_output=True, text=True, timeout=60)
            self.logger.info(str(result.stdout))
            job_file.write(result.stdout)
            job_file.write(result.stderr)

        if result.returncode == 0:
            #self.logger.info("SUCCESS: do not forget to remove the marker file")
            self.logger.info("SUCCESS: removing job marker file: " + job_file_path)
            try:
                os.remove(job_file_path)
            except Exception as e:
                self.logger.error(str(e))
        else: # error
            self.logger.error("Return code " + str(result.returncode))
            self.get_beanstalk_client().bury(job)

    def interrupt_handler(self, signum, frame):
        self.logger.info("interrupt_handler: Received signal: " + str(signum))
        self.logger.info(f'Handling signal {signum} ({signal.Signals(signum).name}).')

        # do some stuff, I guess
        #time.sleep(2)

        sys.exit(0)

    def terminate_handler(self, signum, frame):
        self.logger.info("terminate_handler: Received signal: " + str(signum))
        self.logger.info(f'Handling signal {signum} ({signal.Signals(signum).name}).')

        # do some stuff, I guess
        #time.sleep(2)

        sys.exit(0)
