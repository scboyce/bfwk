#!/bin/bash

CurrentDirectory=$(dirname "$0")
echo "CurrentDirectory: ${CurrentDirectory}"

BfBinFileDirectory=$(realpath ${CurrentDirectory})
echo "BfBinFileDirectory: ${BfBinFileDirectory}"

BfConfDirectory=$(realpath ${BfBinFileDirectory}/../conf)
echo "BfConfDirectory: ${BfConfDirectory}"

ConfigFiles=$(ls -1 ${BfConfDirectory}/*.cfg)

for ConfigFile in ${ConfigFiles}; do
  echo "----------------------------------------"
  ${BfBinFileDirectory}/CreateBatchDirs.sh ${ConfigFile}
done
