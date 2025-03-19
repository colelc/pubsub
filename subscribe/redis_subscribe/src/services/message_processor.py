import subprocess
import os
import time
from src.config.config import Config
from src.logging.app_logger import AppLogger


class MessageProcessor(object):

    def __init__(self, timestamp_marker, dn):
        self.logger = AppLogger.get_logger()
        self.timestamp_marker = timestamp_marker
        self.dn = dn
        self.list_work_directory = Config.get_property("list.work.directory")
        self.list_staging_directory = Config.get_property("list.staging.directory")
   
    def process_message(self) -> int:
        time.sleep(5)
        # here, we want to create the marker file, /tmp/job-[timestamp_marker].out
        # Stdout and Stderr stuff is written to marker file.
        # If the below command is successful, then the marker file is deleted.
        # /bin/sh -cex doIt.sh gid=1335536055293,ou=StaffDisplayGroups,ou=Departments,o=Fuqua,c=US
        #

        job_file_dir = Config.get_property("job.file.directory")
        job_file_name = "job-" + str(self.timestamp_marker) + ".out"
        job_file_path = os.path.join(job_file_dir, job_file_name)
        
        self.logger.info("Preparing job marker file: " + job_file_path)

        #bash_script_dir = os.getenv("PYTHONPATH")
        bash_script_dir = Config.get_property("bash.script.directory")
        bash_script_name = Config.get_property("bash.script.name")
        bash_script_path = os.path.join(bash_script_dir, bash_script_name)

        python_directory = Config.get_property("python.directory")
        python_prog_name = Config.get_property("python.prog.name")
        python_prog_path = os.path.join(python_directory, python_prog_name)

        with open(job_file_path, "w") as job_file:
            self.logger.info("Calling " + str(bash_script_name) + " with dn=" + str(self.dn))
            result = subprocess.run(
                ["/bin/sh", "-e", "-x", bash_script_path, str(self.dn), self.list_work_directory, self.list_staging_directory, python_prog_path], capture_output=True, 
                text=True, 
                timeout=60
            )
            self.logger.info(str(result.stdout))
            job_file.write(result.stdout)
            job_file.write(result.stderr)

        if result.returncode == 0:
            try:
                #self.logger.info("SUCCESS: do not forget to remove the marker file")
                os.remove(job_file_path)
                self.logger.info("SUCCESS: removing job marker file: " + job_file_path)
            except Exception as e:
                self.logger.error(str(e))
        else: # error
            self.logger.error("Return code " + str(result.returncode))

        return (result.returncode, job_file_path)

