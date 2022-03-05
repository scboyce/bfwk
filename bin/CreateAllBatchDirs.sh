#!/bin/bash

ConfigFiles=$(ls -1 /home/ubuntu/bfwk/conf/*.cfg)

for ConfigFile in ${ConfigFiles}; do
  /home/ubuntu/bfwk/bin/CreateBatchDirs.sh ${ConfigFile}
done
