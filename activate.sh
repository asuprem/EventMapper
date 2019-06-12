#!/bin/bash
DIRECTORY="./assed_env"
if [ -d "$DIRECTORY" ]; then
    # Control will enter here if $DIRECTORY exists.
    echo "$(date +"%T") -- assed_env exists. Activating."
    source ./assed_env/bin/activate
else
    echo "$(date +"%T") -- assed_env does not exist."
    echo "$(date +"%T") -- Generating virtual environment assed_env"
    virtualenv -p python3.6 assed_env

    echo "$(date +"%T") -- Activating virtual environment assed_env"
    source ./assed_env/bin/activate
    
    # echo "$(date +"%T") -- Beginning package installs."
    # echo "$(date +"%T") -- Installing basic packages"
    # # Basics
    # pip3 install numpy scipy keras tensorflow 
    # echo "$(date +"%T") -- Installing database packages"
    # # Database/Interconnects
    # pip3 install redis mysqlclient kafka-python
    # echo "$(date +"%T") -- Installing language tools"
    # # Language tools
    # pip3 install gensim nltk sner
    # echo "$(date +"%T") -- Installing interface utilities"
    # # Interface utilities
    # pip3 install click cmd2 
    # echo "$(date +"%T") -- Installing streamer utilities"
    # # Streamer utilities
    # pip3 install newsapi-python tweepy 
    # echo "$(date +"%T") -- Installing web utilities"
    # # Web utilities
    # pip3 install flask requests
    # echo "$(date +"%T") -- Downloading nltk language tools"
    # # Need to download nltk language tool
    # python -c "import nltk; nltk.download('punkt')"

    echo "$(date +"%T") -- Finished."
fi