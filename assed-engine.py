import sys, os, pdb, json
from flask import Flask, request
import click


@click.command()
@click.option("--port", type=int, default=8083)
@click.option("--host", type=str, default="localhost")
def main(port, host):
    do_verify()
    do_initialize()
    app.run(port=port)


app=Flask(__name__)
@app.route("/verify")
def verify():
    """This is to verify whether ASSED is set up properly"""
    status = do_verify()
    resp_val = ""
    if status:
        resp_val = "Verification completed successfully."
    else:
        resp_val = "Verification failed."

    response=app.response_class(response=resp_val, status=200, mimetype="application/json")
    return response

@app.route("/ps")
def ps():
    """This is to list all processes associated with a specific ASSED pipeline"""
    _args_ = {item:request.args[item] for item in request.args}
    print("PS triggered with args: %s"%str(_args_))

@app.route("/initialize")
def initialize():
    """Initialize the backend databases. Create the folders."""
    _args_ = {item:request.args[item] for item in request.args}
    staus = do_initialize(**_args_)

    resp_val = ""
    if status:
        resp_val = "Initialization completed successfully."
    else:
        resp_val = "Initialization failed."

    response=app.response_class(response=resp_val, status=200, mimetype="application/json")
    return response

@app.route("/reset")
def reset():
    """Delete folders. Drop tables. """
    _args_ = {item:request.args[item] for item in request.args}
    print("PS triggered with args: %s"%str(_args_))


def do_verify():
    print("Verify requested.")

    # assume kafka is installed.
    # assume redis is installed.
    # assume mysql is installed.

    return True

def do_initialize(**_args_):
    print("PS triggered with args: %s"%str(_args_))

    return True


    

if __name__ == "__main__":
    main()