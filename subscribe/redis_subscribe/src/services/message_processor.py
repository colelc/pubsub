import json
import subprocess
import os
import time
import urllib.parse
import urllib.request
from src.config.config import Config
from src.logging.app_logger import AppLogger
from src.services.list_rebuilder import ListRebuilder

class MessageProcessor(object):

    def __init__(self, timestamp_marker, dn):
        self.logger = AppLogger.get_logger()
        self.timestamp_marker = timestamp_marker
        self.dn = dn
        self.url_encoded_dn = self.url_encode_string(dn)
        self.list_work_directory = Config.get_property("list.work.directory")
        self.list_staging_directory = Config.get_property("list.staging.directory")

        self.logger.info("Processing message: " + str(dn))
   
    def process_message(self):
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
            # acquire the list name
            url = "https://go.fuqua.duke.edu/fuqua_link/rest/ldap/groupdn/" + self.url_encoded_dn 
            dct = self.rest_api_get(url)

            list_name = self.extract_list_name(dct, url)
            self.logger.info("Extracted list name is: " + list_name)
            if list_name is None:
                return (1, job_file_path)

            # recreate the work directory
            self.logger.info("Recreating work directory: " + self.list_work_directory)
            return_code = self.linux_command(["rm", "-rf", self.list_work_directory], job_file)
            if return_code != 0:
                return (return_code, job_file_path)

            return_code = self.linux_command(["mkdir", self.list_work_directory], job_file)
            if return_code != 0:
                return (return_code, job_file_path)

            # rebuild the list
            self.logger.info("Re-building the " + list_name + " email list")
            ListRebuilder(list_name, self.list_work_directory, self.dn)

            # clean out the staging directory for this email list
            self.logger.info("Cleaning out the staging directory: " + self.list_staging_directory)

            fn = self.build_staging_file_name(list_name)
            aliases = self.build_staging_file_name(list_name, ".aliases")
            authusers = self.build_staging_file_name(list_name, ".authusers")
            config = self.build_staging_file_name(list_name, ".config")
            passwd = self.build_staging_file_name(list_name, ".passwd")

            return_code = self.linux_command(["rm", "-v", "-f", fn, aliases, authusers, config, passwd], job_file)
            if return_code != 0:
                return (return_code, job_file_path)
            
            # copy the work files into the staging directory
            #  (create the staging directory if it does not exist)
            return_code = self.linux_command(["mkdir", "-p", self.list_staging_directory], job_file)
            if return_code != 0:
                return (return_code, job_file_path)
            
            self.logger.info("Copying work files into the staging directory")
            # return_code = self.linux_command(
            #                                 [
            #                                 "find", self.list_work_directory, "-mindepth", "1", "-print0", 
            #                                 "|", "xargs", "-0", "-r", "-I{}", "mv", "-v", "{}", self.list_staging_directory
            #                                 ], 
            #                                 job_file
            #                                 )
            process1 = subprocess.Popen(
                                        [ "find", self.list_work_directory, "-mindepth", "1", "-print0"], 
                                        stdout=subprocess.PIPE
                                        )
            process2 = subprocess.Popen(
                                        ["xargs", "-0", "-r", "-I{}", "mv", "-v", "{}", self.list_staging_directory],
                                        stdin=process1.stdout, 
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE
                                        )
            
            process1.stdout.close()
            #output = process2.communicate()[0]
            output, error = process2.communicate()
            if process2.returncode != 0:
                self.logger.error(str(error.decode()))
                return (process2.returncode, job_file_path)
            
            # self.logger.info(str(type(output)))
            # self.logger.info(str(output))
            self.logger.info(str(output.decode()))
            # if return_code != 0:
            #     return (return_code, job_file_path)
            


            ########################################################################################
        #     self.logger.info("Calling " + str(bash_script_name) + " with dn=" + str(self.dn))
        #     result = subprocess.run(
        #         ["/bin/sh", "-e", "-x", bash_script_path, str(self.dn), self.list_work_directory, self.list_staging_directory, python_prog_path], capture_output=True, 
        #         text=True, 
        #         timeout=60
        #     )
        #     self.logger.info(str(result.stdout))
        #     job_file.write(result.stdout)
        #     job_file.write(result.stderr)

        # if result.returncode == 0:
        #     try:
        #         #self.logger.info("SUCCESS: do not forget to remove the marker file")
        #         os.remove(job_file_path)
        #         self.logger.info("SUCCESS: removing job marker file: " + job_file_path)
        #     except Exception as e:
        #         self.logger.error(str(e))
        # else: # error
        #     self.logger.error("Return code " + str(result.returncode))

        return (0, job_file_path)

    def url_encode_string(self, input_string):
        encoded_string = urllib.parse.quote(input_string)
        return encoded_string
    
    def rest_api_get(self, url):
        try:
            headers = {"Accept": "application/json"}
            req = urllib.request.Request(url, headers=headers, method="GET")

            with urllib.request.urlopen(req) as response:
                json_string = response.read().decode("UTF-8")
                return json.loads(json_string) #dict  
            
        except Exception as err:
            self.logger.error("EXCEPTION")
            self.logger.error(str(type(err)))
            self.logger.error(str(err))
            self.logger.error(str(err.__dict__))

    def extract_list_name(self, dct, url) -> str:
        if dct is None:
            return None

        #for k,v in dct.items():
        #    self.logger.info(k + " -> " + str(v))

        success = dct["success"]
        if success is None or success == False:
            self.logger.error("URL: " + url + " -> success: False, returning")
            return None

        group = dct["group"]
        if group is None:
            self.logger.error("URL: " + url + " -> success: True, but no group, returning")
            return None
        
        for k,v in group.items():
            self.logger.info("group: " + k + " -> " + str(v))

        mail = group["mail"]
        #self.logger.info("mail: " + mail)

        if mail is None:
            self.logger.error("URL: " + url + " -> success: True, but no value for group email, returning")
            return None

        list_name = mail.split("@", 1)[0].lower()
        #self.logger.info("list name: " + list_name)
        return list_name

    def linux_command(self, cmd, job_file) -> int:
        #self.logger.info(str(cmd))
        self.logger.info(" ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        #self.logger.info(str(result.stdout))
        job_file.write(result.stdout)
        job_file.write(result.stderr)

        if result.returncode != 0:
            self.logger.error("non-zero return code for command: " + str(cmd))

        return result.returncode
    
    def build_staging_file_name(self, list_name, ext=None) -> str:
        if ext is None:
            fn = os.path.join(self.list_staging_directory, list_name)
            return fn
        
        fn = os.path.join(self.list_staging_directory, list_name) + ext
        return fn
        



