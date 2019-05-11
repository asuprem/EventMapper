import click
import utils.helper_utils as helper_utils
import time

@click.command()
@click.argument("importkey")
def main(importkey):
    helper_utils.std_flush(importkey)

    while True:
        time.sleep(10)
        helper_utils.std_flush(importkey)


if __name__ == "__main__":
    main()  #pylint: disable=no-value-for-parameter
