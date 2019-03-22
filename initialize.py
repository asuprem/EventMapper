# This initializees several things...
import pdb
import argparse
import glob
from utils import db_utils
from utils.file_utils import load_config

parser = argparse.ArgumentParser(description="Initialize sets up various parts of LITMUS")
parser.add_argument("--env",
                    choices=["mysql", "dirs"],
                    help="Environment to setup")

argums = vars(parser.parse_args())
config = load_config()

if argums['env'] == 'dirs':
    import os
    dirs = ['downloads','logfiles']
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)

if argums['env'] == 'mysql':
    #set up mysql stuff (news and everything)
    db_conn = db_utils.get_db_connection(config)
    for file_ in glob.glob('initialization/mysql/*.SQL'):
        db_utils.run_sql_file(file_,db_conn)
        db_conn.close()
        
