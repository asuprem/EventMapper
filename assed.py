''' ASSED - Adaptive Social Sensor Event Detection 

This is the server for ASSED. Once it begins running, it should begin taking requests from a user.
'''

# General Imports
import os, json, click
import utils.helper_utils as helper_utils, utils.file_utils as file_utils, utils.db_utils as db_utils
import utils.AssedPipeline as AssedPipeline
import utils.CONSTANTS as CONSTANTS

@click.command()
@click.argument("assedtopic")
def main(assedtopic):
    assedtopic = "landslide"
    configuration = file_utils.load_config(CONSTANTS.ASSED_CONFIG)

    pipeline_config_name = configuration["topic_names"][assedtopic]["pipeline"]["src"] + ".json"
    pipeline_configuration = file_utils.load_config("./config/assed_pipelines/"+ pipeline_config_name)
    assed_pipeline = AssedPipeline.AssedPipeline(pipeline_configuration)
    assed_pipeline.run()

    helper_utils.std_flush("Finished")




    







if __name__ == "__main__":
    main()

