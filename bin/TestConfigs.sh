#!/bin/bash

for file in ../conf/*.cfg; do
   echo "==========================================="
   echo $file
   . $file
   echo
done
