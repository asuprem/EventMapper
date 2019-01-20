# This initializees several things...
import pdb
import argparse
import glob
import utils

parser = argparse.ArgumentParser(description="Initialize sets up various parts of LITMUS")
parser.add_argument("--env",
                    choices=["mysql", "dirs"],
                    help="Environment to setup")

argums = vars(parser.parse_args())
config = utils.load_config()

if argums['env'] == 'dirs':
    import os
    dirs = ['downloads','logfiles']
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)

if argums['env'] == 'mysql':
    #set up mysql stuff (news and everything)
    db_conn = utils.get_db_connection(config)
    for file_ in glob.glob('initialization/mysql/*.SQL'):
        utils.run_sql_file(file_,db_conn)
        db_conn.close()
        
