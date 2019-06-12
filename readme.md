# WIP

# Requirements

1. Working directory must be empty (probably not, but let's not get ahead of ourselves).
2. Create a new virtual environment andd activate it:
    $ virtualenv -p /path/to/python3.6.8 assed_env
    $ source assed_env/bin/activate
3. Create the configuration folder with the configuration file assed_config.json

To decide
    when does the engine set up kafka, redis, mysql
    and when does the engine access their details in config
    ???


- Approach - load the config file and extract dependencies
    - for each dependency, access assed.dependencies.check_{} i.e. assed.dependencies.check_mysql, assed.dependencies.check_redis, etc...
    - then for each dependency, run the initialization scripts
    - config-- {"dependencies":...}

set _config = (loadconfig <config>)
set _dependencies = (dot _config dependencies)
foreach _dependencies as _subdependency
set _depres = (checkdependency _subdependency),
if _depres false
error(ERROR STRING)




better approach - have assed-engine perform basic management functions



So what do i need to do?
    - AssedEngine is a class...
    - methods in class:
    





