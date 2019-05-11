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
        if not os.path.exists(self.log_dir):
            helper_utils.std_flush("Log directory not created. Creating")
            os.makedirs(self.log_dir)

        helper_utils.std_flush("Finished verifying log directory %s"%self.log_dir)

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
        
        pdb.set_trace()

    def createProcessScripts(self):
        pass

    def createOutputBufferSscript(self):
        pass    
    
    def run(self,):
        # Initiate each of the config executables.
        # Assume they all require python and are executed through assed_env
        pdb.set_trace()

        # Launch Input Buffer


        # Launch Output Buffer

        # Launch Each Process
