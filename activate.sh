#!/bin/bash
if ! [ -x "$(command -v virtualenv)" ]; then
  echo 'Error: virtualenv not installed'
  pip3 install virtualenv
fi
DIRECTORY="./assed_env"
if [ -d "$DIRECTORY" ]; then
    # Control will enter here if $DIRECTORY exists.
    echo "$(date +"%T") -- assed_env exists."
    # remove the assed_env and generate it again
    echo "$(date +"%T") -- remove assed_env."
    rm -rf assed_env 
    virtualenv -p python3.6 assed_env

else
    echo "$(date +"%T") -- assed_env does not exist."
    echo "$(date +"%T") -- Generating virtual environment assed_env"
    virtualenv -p python3.6 assed_env
fi
    
echo "$(date +"%T") -- Activating virtual environment assed_env"
source ./assed_env/bin/activate

if [ $? -eq 0 ]; then
    echo "$(date +"%T") -- Beginning package installs."
    echo "$(date +"%T") -- Installing basic packages"
    # Basics
    pip3 install numpy scipy
    pip3 install keras==2.2.4
    echo "$(date +"%T") -- Installing tensorflow 1.5"
    # Tensorflow TODO Add a better check. Tensorflow>1.5 requires AVX instructions, which test rig does not have.
    pip3 install tensorflow==1.5
    
    echo "$(date +"%T") -- Installing database packages"
    # Database/Interconnects
    pip3 install mysqlclient
    if [ $? -eq 0 ]; then
        :
    else
        echo "Installing mysqlclient failed."
        exit 1
    fi
    
    echo "$(date +"%T") -- Installing database packages"
    # Database/Interconnects
    pip3 install redis kafka-python
    echo "$(date +"%T") -- Installing language tools"
    # Language tools
    pip3 install gensim nltk sner
    echo "$(date +"%T") -- Installing interface utilities"
    # Interface utilities
    pip3 install click cmd2 
    echo "$(date +"%T") -- Installing streamer utilities"
    # Streamer utilities
    pip3 install newsapi-python tweepy 
    echo "$(date +"%T") -- Installing standard utilities"
    # Web utilities
    pip3 install xmltodict==0.12.0 reverse_geocoder==1.5.1
    echo "$(date +"%T") -- Installing web utilities"
    # Web utilities
    pip3 install flask requests
    echo "$(date +"%T") -- Downloading nltk language tools"
    # Need to download nltk language tool
    python -c "import nltk; nltk.download('punkt')"
else
    echo "FAILED TO ACTIVATE virtual environment $DIRECTORY."
    exit 1
fi


echo "$(date +"%T") -- Finished."
