import pdb
pdb.set_trace()
import click
import utils.helper_utils as helper_utils
import time, os, sys

@click.command()
@click.argument("importkey")
def main(importkey):
    pdb.set_trace()
    pid_name = os.path.basename(sys.argv[0]).split('.')[0]
    helper_utils.setup_pid(pid_name)
    
    helper_utils.std_flush(importkey)


    while True:
        time.sleep(10)
        helper_utils.std_flush(importkey)


if __name__ == "__main__":
    main()  #pylint: disable=no-value-for-parameter
