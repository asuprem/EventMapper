''' ASSED - Adaptive Social Sensor Event Detection 

This is the server for ASSED. Once it begins running, it should begin taking requests from a user.
'''

# General Imports
import os, json, click
import utils.helper_utils as helper_utils, utils.file_utils as file_utils, utils.db_utils as db_utils
import utils.ASSED.AssedPipeline as AssedPipeline
import utils.CONSTANTS as CONSTANTS

@click.command()
@click.argument("assedtopic")
def main(assedtopic):
    assedtopic = "landslide"
    manager = {}

    manager[assedtopic] = {}

    assed_config = file_utils.load_config(CONSTANTS.ASSED_CONFIG)

    pipeline_config_name = assed_config["topic_names"][assedtopic]["pipeline"]["src"] + ".json"
    manager[assedtopic]["pipeline_configuration"] = file_utils.load_config("./config/assed_pipelines/"+ pipeline_config_name)


    manager[assedtopic]["assed_pipeline"] = AssedPipeline.AssedPipeline(assed_config["home"], manager[assedtopic]["pipeline_configuration"], mode="DEBUG")
    manager[assedtopic]["assed_pipeline"].run()

    helper_utils.std_flush("Finished")



if __name__ == "__main__":
    main()  #pylint: disable=no-value-for-parameter

