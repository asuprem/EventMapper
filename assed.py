''' ASSED - Adaptive Social Sensor Event Detection 

This is the server for ASSED. Once it begins running, it should begin taking requests from a user.
'''

# General Imports
import os, json, click
import utils.helper_utils as helper_utils, utils.file_utils as file_utils, utils.db_utils as db_utils
import utils.Pipeline as Pipeline
import utils.CONSTANTS as CONSTANTS

@click.command()
@click.argument("assedtopic")
def main(assedtopic):

    configuration = file_utils.load_config(CONSTANTS.ASSED_CONFIG)
    assed_pipeline = Pipeline.Pipeline()




    







if __name__ == "__main__":
    main()

