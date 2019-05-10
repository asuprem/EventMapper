import sys, time, os, json, codecs, traceback
import pdb
from datetime import datetime


import multiprocessing
from utils.file_utils import load_config
from utils.helper_utils import dict_equal, setup_pid, readable_time, std_flush
import utils.CONSTANTS as CONSTANTS


if __name__ == '__main__':
    pid_name = os.path.basename(sys.argv[0]).split('.')[0]
    setup_pid(pid_name)
    
    assed_config = load_config(CONSTANTS.ASSED_CONFIG)

    configOriginal = load_config(CONSTANTS.HIGH_CONFIDENCE_CONFIG_PATH)

    
    HCS_configuration = {}
    errorQueue = multiprocessing.Queue()
    messageQueue = multiprocessing.Queue()

    for hcs_type in configOriginal:
        _cfg = configOriginal[hcs_type]
        kwargs = {}
        HCS_configuration[hcs_type] = {}
        HCS_configuration[hcs_type]["name"] = _cfg["name"]
        HCS_configuration[hcs_type]["db_name"] = _cfg["db_name"]
        HCS_configuration[hcs_type]["source_file"] = _cfg["source_file"]
        HCS_configuration[hcs_type]["type"] = _cfg["type"]
        if HCS_configuration[hcs_type]["type"] == "scheduled":
            HCS_configuration[hcs_type]["schedule"] = _cfg["schedule"]
        if "config" in _cfg:
            HCS_configuration[hcs_type]["config"] = load_config(_cfg["config"])
            kwargs["config"] = HCS_configuration[hcs_type]["config"]

        # Now we have stuff setup. We will launch the thingamajigs here
        # Perform the import, then execute
        moduleImport = __import__("HighConfidenceStreamerSrc.%s"%_cfg["source_file"], fromlist=[_cfg["source_file"]])
        Executor = getattr(moduleImport, _cfg["source_file"])

        try:
            HCS_configuration[hcs_type]['processor'] = Executor(assed_config, root_name=hcs_type, errorQueue=errorQueue, messageQueue=messageQueue, **kwargs)
        except Exception as e:
            traceback.print_exc()
            std_flush("Failed to launch %s with error %s"%(hcs_type, repr(e)))
        std_flush("Launch complete for ", hcs_type, "HighConfigurationStreamer at ",readable_time())
        HCS_configuration[hcs_type]['processor'].start()
        HCS_configuration[hcs_type]['timestamp'] = time.time()
    
    configCheckTimer = time.time()

    while True:
        if time.time() - configCheckTimer > CONSTANTS.HCS_CONFIG_TIME_CHECK:
            configCheckTimer = time.time()
            std_flush( " ".join(["Checking configuration at", readable_time()]))
            configReload = load_config(CONSTANTS.HIGH_CONFIDENCE_CONFIG_PATH)
            configCheckTimer = time.time()
            # TODO handle config changes...
            pass
        
        # Rerun scheduled ones
        for hcs_type in HCS_configuration:
            if HCS_configuration[hcs_type]["type"] == "scheduled":
                # Rerun if we are at schedule
                if time.time() - HCS_configuration[hcs_type]["timestamp"] > HCS_configuration[hcs_type]["schedule"]:
                    std_flush("Beginning", hcs_type, "HighConfigurationStreamer scheduled launch at ",readable_time())
                    try:
                        HCS_configuration[hcs_type]["processor"].terminate()
                        std_flush("Terminated possible zombie process for ", hcs_type, " at ",readable_time())
                    except:
                        pass
                    # Set up kwargs...
                    kwargs = {}
                    if "config" in HCS_configuration[hcs_type]:
                        kwargs["config"] = HCS_configuration[hcs_type]["config"]
                    try:
                        HCS_configuration[hcs_type]['processor'] = Executor(assed_config, root_name=hcs_type, errorQueue=errorQueue, messageQueue=messageQueue, **kwargs)
                    except Exception as e:
                        std_flush("Failed to launch %s with error %s"%(hcs_type, repr(e)))
                    HCS_configuration[hcs_type]['processor'].start()
                    std_flush("Scheduled launch complete for ", hcs_type, "HighConfigurationStreamer at ",readable_time())
                    HCS_configuration[hcs_type]['timestamp'] = time.time()
        

        while not errorQueue.empty():
            #TODO get error, time, restart
            _rootName, _error = errorQueue.get()
            std_flush(" ".join([_rootName, "crashed with error: ", str(_error)]))
            try:
                HCS_configuration[_rootName]['processor'].terminate()
            except:
                pass
            std_flush(" ".join(["Shutdown",str(_rootName), "at", readable_time()]))
        
        # if vs while -- prevents locking here if a process keeps sending messages... but what about overfill? handle that later... TODO
        if not messageQueue.empty():
            std_flush(messageQueue.get())
                        


