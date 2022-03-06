#!/bin/bash
###############################
# CreateBatchDirs.sh

#-- Process options if any
Usage="Usage CreateBatchDirs.sh <BatchConfigFile>"
ConfigFile="${1}"
#echo "ConfigFile: ${ConfigFile}"

if [[ -r ${ConfigFile} ]]; then
   BatchName=$(grep "^BatchName=" ${ConfigFile} | cut -d"=" -f2)
   #echo "BatchName: ${BatchName}"
else
   echo "${Usage}"
   exit 1
fi

echo "Creating Batch Directories for: ${BatchName}"

mkdir -p /home/ubuntu/bfwk/bin/${BatchName}
mkdir -p /home/ubuntu/bfwk/data/${BatchName}/poll
mkdir -p /home/ubuntu/bfwk/data/${BatchName}/work
mkdir -p /home/ubuntu/bfwk/log/${BatchName}
