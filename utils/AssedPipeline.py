import time, os, sys, traceback, redis
import pdb

import utils.helper_utils as helper_utils

class AssedPipeline():

    """ This is an ASSED Pipeline class..."""
    def __init__(self,pipeline_config):
        self.config = pipeline_config

        # Create Log directory
        self.log_dir = "./logfiles/" + self.config["configuration"]["log_dir"]
        if not os.path.exists(self.log_dir):
            helper_utils.std_flush("Log directory not created. Creating")
            os.makedirs(self.log_dir)

        helper_utils.std_flush("Finished verifying log directory %s"%self.log_dir)

        # Create Scripts
        pass
        
    def run(self,):
        # Initiate each of the config executables.
        # Assume they all require python and are executed through assed_env
        pdb.set_trace()

        # Launch Input Buffer


        # Launch Output Buffer

        # Launch Each Process
