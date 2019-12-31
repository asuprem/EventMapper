#!/bin/bash
# go in ./download/tweet_download_2019.../ then execute this
for d in */ ; do
    cd $d
	count=$(cat */*/*.json | wc -l)
	echo "$d,$count"
done