import cmd2
import pdb
import click
import requests

class ASSED(cmd2.Cmd):
    prompt = "assed> "


    def __init__(self, port, host):
        cmd2.Cmd.__init__(self)
        self.port = port
        self.host = host

    def do_verify(self, _args_):
        """Verify the ASSED installation. Connects to the ASSED-port specified to ensure REDIS and Kafka are in smooth operation and files are valid.
        
        Usage:
                    
        $ verify
        
        """
        self.make_request("verify", _args_)

    def make_request(self, keyword, arg):
        resp = requests.get('''http://{host}:{port}/{keyword}'''.format(host=self.host, port=self.port, keyword=keyword), params={"args":arg})
        if resp.status_code == 200:
            print(resp.text)
        else:
            print("{keyword} -- failed with response:\n\n{message}".format(keyword=keyword, message=resp.text))


@click.command()
@click.option("--port", type=int, default=8083)
@click.option("--host", type=str, default="localhost")
def main(port, host):
    app = ASSED(port, host)
    app.cmdloop()


if __name__ == "__main__":
    main()