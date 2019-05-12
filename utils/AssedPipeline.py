import time, os, sys, traceback, redis, kafka
import pdb
import subprocess
import utils.helper_utils as helper_utils

class AssedPipeline():

    """ This is an ASSED Pipeline class..."""
    def __init__(self, home_dir, pipeline_config, mode="DEBUG"):
        self.config = pipeline_config
        self.home_dir = home_dir
        if mode == "DEBUG" or mode == "production":
            self.mode = mode
        else:
            raise ValueError("Unrecognized mode. Set mode to one of: 'DEBUG', 'production'.")
        # Create Log directory TODO Sanitize
        self.log_dir = "./logfiles/" + self.config["configuration"]["log_dir"]
        self.script_dir = "./pipelines/" + self.config["configuration"]["script_dir"]
        self.assed_sript_dir = "./pipelines"
        self.sh_dir = "./scripts/" + self.config["configuration"]["sh_dir"]
        self.script_dir_importname = self.config["configuration"]["script_dir"]

        self.createIfNotExists(self.log_dir)
        self.createIfNotExists(self.script_dir)
        self.createIfNotExists(self.sh_dir)
        
        # Script files
        self.inputBufferScriptFile = None
        self.outputBufferScriptFile = None
        self.processScripts = []

        # set up script deets
        self.input_scripts = ['input_buffer']
        self.output_scripts = ['output_buffer']
        self.process_scripts = []
        for script_type in self.config:
            if script_type not in self.input_scripts + self.output_scripts + ['configuration']:
                self.process_scripts.append(script_type)

        # Set up kafka keys:
        self.initializeKafka()
        # If debug, delete assed keys
        #self.deleteRedisKeys()

        # Create Scripts
        self.createInputBufferScript()
        self.createProcessScripts()
        self.createOutputBufferScript()
    
    def initializeKafka(self):
        admin = kafka.admin.KafkaAdminClient()
        for _scriptref in self.input_scripts + self.output_scripts + self.process_scripts:
            kafka_key = self.config[_scriptref]["export-key"].replace(":","_")
            try:
                admin.create_topics(new_topics=[kafka.admin.NewTopic(name=kafka_key, num_partitions=1, replication_factor=1)], validate_only=False)
                helper_utils.std_flush("Created %s export key in kafka broker"%kafka_key)
            except kafka.errors.TopicAlreadyExistsError:
                helper_utils.std_flush("%s exportkey already exists in Kafka broker"%kafka_key)

    def deleteRedisKeys(self,):
        if self.mode == "production":
            return
        pool = redis.ConnectionPool(host='localhost',port=6379, db=0)
        r=redis.Redis(connection_pool = pool)
        # TODO Fix this...maybe remove; probably no longer needed...
        #for _scriptref in self.input_scripts + self.output_scripts + self.process_scripts:
        for _scriptref in self.output_scripts + self.process_scripts:
            r_offset = self.config[_scriptref]["export-key"]+":offset"
            r_partition = self.config[_scriptref]["export-key"]+":partition"
            r_timestamp = self.config[_scriptref]["export-key"]+":timestamp"
            r.delete(r_offset)
            r.delete(r_partition)
            r.delete(r_timestamp)

    def createInputBufferScript(self):
        for _inputscript in self.input_scripts:
            scriptname = self.config[_inputscript]["script"]
            inputbuffername = self.config[_inputscript]["name"]
            exportkey = self.config[_inputscript]["export-key"]
            bufferStr = \
            '''#!/bin/sh
cd {homedir}
if ps up `cat {logdir}/{inputbuffername}.pid ` > /dev/null
then
    printf "{inputbufferscriptname}.py is aleady running\\n" >> {logdir}/{inputbuffername}.out
else
    printf "{inputbuffername} is no longer running. Deleting PID file.\\n" >> {logdir}/{inputbuffername}.out
    rm  {logdir}/{inputbuffername}.pid >> {logdir}/{inputbuffername}.out
    printf "Deleted file\\n" >> {logdir}/{inputbuffername}.out
    printf "Starting {inputbuffername}.py\\n" >> {logdir}/{inputbuffername}.out
    nohup ./assed_env/bin/python {scriptdir}/{inputbufferscriptname}.py {logdir} {exportkey} {pidname} >> {logdir}/{inputbuffername}.log 2>&1 &
fi'''.format(homedir = self.home_dir, logdir = self.log_dir, inputbufferscriptname = scriptname, inputbuffername = inputbuffername, scriptdir = self.script_dir, exportkey = exportkey, pidname=inputbuffername)
        

        self.inputBufferScriptFile = os.path.join(self.sh_dir, scriptname + ".sh")
        self.writeScript(self.inputBufferScriptFile, bufferStr)
        helper_utils.std_flush("Generated script for Input Buffer at %s"%self.inputBufferScriptFile)

    def createProcessScripts(self):
        for _processscript in self.process_scripts:
            scriptname = self.config[_processscript]["script"]
            processname = self.config[_processscript]["name"]
            importkey = self.config[_processscript]["import-key"]
            exportkey = self.config[_processscript]["export-key"]
            bufferStr = \
            '''#!/bin/sh
cd {homedir}
if ps up `cat {logdir}/{processname}.pid ` > /dev/null
then
    printf "{processscriptname}.py is aleady running\\n" >> {logdir}/{processname}.out
else
    printf "{processname} is no longer running. Deleting PID file.\\n" >> {logdir}/{processname}.out
    rm  {logdir}/{processname}.pid >> {logdir}/{processname}.out
    printf "Deleted file\\n" >> {logdir}/{processname}.out
    printf "Starting {processname}.py\\n" >> {logdir}/{processname}.out
    nohup ./assed_env/bin/python {assedscript}/assed_process.py {logdir} {importkey} {exportkey} {processscriptname} {processscriptdir} {pidname} >> {logdir}/{processname}.log 2>&1 &
fi'''.format(homedir = self.home_dir, logdir = self.log_dir, processscriptname = scriptname, processname = processname, assedscript = self.assed_sript_dir, exportkey = exportkey, importkey = importkey, processscriptdir = self.script_dir_importname, pidname=processname)
        

            self.inputBufferScriptFile = os.path.join(self.sh_dir, scriptname + ".sh")
            self.writeScript(self.inputBufferScriptFile, bufferStr)
            helper_utils.std_flush("Generated script for %s  at %s"%(_processscript, self.inputBufferScriptFile))

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
        
        # Launch Input Buffer -- run the input buffer script
        #subprocess.Popen(['sh', self.inputBufferScriptFile])

        # Launch Output Buffer
        pdb.set_trace()
        # Launch Each Process
