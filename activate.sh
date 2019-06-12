#!/bin/bash
$ASSED_ENV="./assed_env"
if [ -d "$DIRECTORY" ]; then
    # Control will enter here if $DIRECTORY exists.
    source ./assed_env/bin/activate
else
    echo "Directory $DIRECTORY does not exist"
fi