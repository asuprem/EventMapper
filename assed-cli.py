import cmd2
import pdb
import click
import requests

class ASSED(cmd2.Cmd):
    prompt = "assed> "


    def __init__(self):
        cmd2.Cmd.__init__(self)

    def do_verify(self, _args_):
        """Verify the ASSED installation. Connects to the ASSED-port specified to ensure REDIS and Kafka are in smooth operation and files are valid. 
        
Usage:
        
$ verify
        
"""
        pass
        print(_args_)



@click.command()
@click.option("--port", type=int)
@click.option("--host", type=str, default="localhost")
def main():
    app = ASSED()
    app.cmdloop()


if __name__ == "__main__":
    main()