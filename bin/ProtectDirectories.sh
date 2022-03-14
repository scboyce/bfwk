#!/bin/bash

CurrentDirectory=$(dirname "$0")
echo "CurrentDirectory: ${CurrentDirectory}"

BfDirectory=$(realpath ${CurrentDirectory}/..)
echo "BfDirectory: ${BfDirectory}"

#-- Directories
echo "chmoding directories..."

find ${BfDirectory}/conf/ -type d -exec chmod 700 {} \;

find ${BfDirectory}/bin/  -type d -exec chmod 750 {} \;
find ${BfDirectory}/data/ -type d -exec chmod 750 {} \;
find ${BfDirectory}/lock/ -type d -exec chmod 750 {} \;

find ${BfDirectory}/log/  -maxdepth 1 -type d -exec chmod 750 {} \;

#-- Files
echo "chmoding bin files..."

find ${BfDirectory}/bin/ -type f -name "*.sh"   -exec chmod 750 {} \;
find ${BfDirectory}/bin/ -type f -name "*.pl"   -exec chmod 750 {} \;
find ${BfDirectory}/bin/ -type f -name "*.pm"   -exec chmod 750 {} \;
find ${BfDirectory}/bin/ -type f -name "*.py"   -exec chmod 750 {} \;
find ${BfDirectory}/bin/ -type f -name "*.R"    -exec chmod 750 {} \;
find ${BfDirectory}/bin/ -type f -name "*.proc" -exec chmod 640 {} \;
find ${BfDirectory}/bin/ -type f -name "*.txt"  -exec chmod 640 {} \;

echo "chmoding conf files..."

find ${BfDirectory}/conf/ -type f -exec chmod 600 {} \;

echo "Done"
