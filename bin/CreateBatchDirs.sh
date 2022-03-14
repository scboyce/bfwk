#!/bin/bash

#-- Process options if any
Usage="Usage CreateBatchDirs.sh <BatchConfigFile>"
ConfigFile="${1}"
echo "ConfigFile: ${ConfigFile}"

if [[ -r ${ConfigFile} ]]; then

   #-- Source it
   . ${ConfigFile}

   echo "ApplicationName: ${ApplicationName}"
   echo "BatchName: ${BatchName}"

   echo "BinFileDirectory : ${BinFileDirectory}"
   echo "LogFileDirectory : ${LogFileDirectory}"
   echo "DataFileDirectory: ${DataFileDirectory}"
   echo "WorkFileDirectory: ${WorkFileDirectory}"
   echo "PollFileDirectory: ${PollFileDirectory}"

   echo "Creating Batch Directories for: ${BatchName}"
   mkdir -p ${BinFileDirectory}
   mkdir -p ${LogFileDirectory}
   mkdir -p ${WorkFileDirectory}
   mkdir -p ${PollFileDirectory}

   echo "Done"

else
   echo "${Usage}"
   exit 1
fi
