# Initialization

## Directories
Initialize can also set up some directories if they are not there already.

These include **downloads**, 

### Setting up directories

Run the following

    venv/bin/python initialize --env dirs

## MYSQL
We need to set up MYSQL (databases, not the actual server) before running any programs or setting up the rest of the cron jobs. Look through `readme` to learn how to start/access MySQL. The initialization scripts assume MySQL is already running and `config.json` contains the correct usernames and passwords.

### Setting up MySQL

Run the following:

    venv/bin/python initialize.py --env mysql