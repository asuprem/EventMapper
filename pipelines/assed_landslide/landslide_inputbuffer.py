import sys, os
sys.path.append(os.getcwd())

import click
import utils.helper_utils as helper_utils
import time, pdb

@click.command()
@click.argument("logdir")
@click.argument("exportkey")
def main(logdir, exportkey):
    pid_name = os.path.basename(sys.argv[0]).split('.')[0]
    helper_utils.setup_pid(pid_name, logdir=logdir)
    
    helper_utils.std_flush(exportkey)


    while True:
        time.sleep(10)
        helper_utils.std_flush(exportkey)


if __name__ == "__main__":
    main()  #pylint: disable=no-value-for-parameter
