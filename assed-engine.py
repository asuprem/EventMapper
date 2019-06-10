import sys, os


from flask import Flask, request
app=Flask(__name__)

@app.route("/verify")
def verify():
    """This is to verify whether ASSED is set up properly"""
    print("Verify triggered")

@app.route("/ps")
def ps():
    """This is to list all processes associated with a specific ASSED pipeline"""
    _args_ = {item:request.args[item] for item in request.args}
    print("PS triggered with args: %s"%str(_args_))