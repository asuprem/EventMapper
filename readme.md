# LITMUS
This directory contains scripts for LITMUS (updated).

Details will be added over time.


# Configuration
`config.json` contains the configuration file for LITMUS, along with all passwords, etc

# MYSQL Access
Start the mysql server with
    sudo /etc/init.d/mysqld  start

Access the mysql server with 
    > mysql -h 127.0.0.1 -P 3306 -u grait-dm -p
    > Lr1eUDc(f4Hi

The format is:
    mysql -h HOST -P PORT_NUMBER -u USERNAME -p

The config file is located at `/etc/my.cnf`. The socket is located at `/var/lib/mysql/mysql.sock`.

## MYSQL DB
All our work is done on the LITMUS database in mysql. Use `use LITMUS` when logging in to switch to the correct database
