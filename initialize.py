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
assed_config = load_config('config/assed_config.json')


if argums['env'] == 'dirs':
    import os
    dirs = ['downloads','logfiles', 'config', 'redis', 'ml', 'ml/models', 'ml/encoders']
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)

if argums['env'] == 'mysql':
    #set up mysql stuff (news and everything)
    db_conn = db_utils.get_db_connection(assed_config)
    for file_ in glob.glob('initialization/mysql/*.SQL'):
        print("Initializing SQL from %s"%file_)
        db_utils.run_sql_file(file_,db_conn)
    db_conn.close()
        
