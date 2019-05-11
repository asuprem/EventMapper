import time, os, sys, traceback, redis
import pdb

import utils.helper_utils as helper_utils

class AssedPipeline():

    """ This is an ASSED Pipeline class..."""
    def __init__(self, home_dir, pipeline_config):
        self.config = pipeline_config
        self.home_dir = home_dir

        # Create Log directory TODO Sanitize
        self.log_dir = "./logfiles/" + self.config["configuration"]["log_dir"]
        self.script_dir = "./pipelines/" + self.config["configuration"]["script_dir"]
        self.sh_dir = "./scripts/" + self.config["configuration"]["sh_dir"]

        self.createIfNotExists(self.log_dir)
        self.createIfNotExists(self.script_dir)
        self.createIfNotExists(self.sh_dir)
        

        

        # Create Scripts
        self.createInputBufferScript()
        self.createProcessScripts()
        self.createOutputBufferScript()
        
    def createInputBufferScript(self):
        scriptname = self.config["input_buffer"]["script"]
        exportkey = self.config["input_buffer"]["export-key"]
        bufferStr = \
        '''#!/bin/sh
cd {homedir}
if ps up `cat {logdir}/{inputbuffername}.pid ` > /dev/null
then
    printf "{inputbuffername}.py is aleady running\\n" >> {logdir}/{inputbuffername}.out
else
    printf "{inputbuffername} is no longer running. Deleting PID file.\\n" >> {logdir}/{inputbuffername}.out
    rm  {logdir}/{inputbuffername}.pid >> {logdir}/{inputbuffername}.out
    printf "Deleted file\\n" >> {logdir}/{inputbuffername}.out
    printf "Starting {inputbuffername}.py\\n" >> {logdir}/{inputbuffername}.out
    nohup ./assed_env/bin/python {scriptdir}/{inputbuffername}.py {exportkey} >> {logdir}/{inputbuffername}.log 2>&1 &
fi'''.format(homedir = self.home_dir, logdir = self.log_dir, inputbuffername = scriptname, scriptdir = self.script_dir, exportkey = exportkey)
        

        inputBufferScriptFile = os.path.join(self.sh_dir, scriptname + ".sh")
        self.writeScript(inputBufferScriptFile, bufferStr)
        helper_utils.std_flush("Generated script for Input Buffer at %s"%inputBufferScriptFile)

    def createProcessScripts(self):
        pass

    def createOutputBufferScript(self):
        pass    

    def createIfNotExists(self,dir_):
        if not os.path.exists(dir_):
            helper_utils.std_flush("%s directory not created. Creating"%dir_)
            os.makedirs(dir_)
        helper_utils.std_flush("Finished verifying directory %s"%dir_)

    def writeScript(self,filename, script_str):
        with open(filename, 'w') as file_:
            file_.write(script_str)
    
    def run(self,):
        # Initiate each of the config executables.
        # Assume they all require python and are executed through assed_env
        pdb.set_trace()

        # Launch Input Buffer


        # Launch Output Buffer

        # Launch Each Process
