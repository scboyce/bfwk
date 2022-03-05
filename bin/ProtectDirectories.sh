#!/bin/bash

#-- Directories
find /home/ubuntu/bfwk/bin/  -type d -exec chmod 750 {} \;
find /home/ubuntu/bfwk/conf/ -type d -exec chmod 700 {} \;
find /home/ubuntu/bfwk/data/ -type d -exec chmod 750 {} \;
find /home/ubuntu/bfwk/lock/ -type d -exec chmod 750 {} \;
find /home/ubuntu/bfwk/log/  -type d -exec chmod 750 {} \;

#-- Files
find /home/ubuntu/bfwk/bin/ -type f -name "*.sh"   -exec chmod 750 {} \;
find /home/ubuntu/bfwk/bin/ -type f -name "*.pl"   -exec chmod 750 {} \;
find /home/ubuntu/bfwk/bin/ -type f -name "*.pm"   -exec chmod 750 {} \;
find /home/ubuntu/bfwk/bin/ -type f -name "*.py"   -exec chmod 750 {} \;
find /home/ubuntu/bfwk/bin/ -type f -name "*.R"    -exec chmod 750 {} \;
find /home/ubuntu/bfwk/bin/ -type f -name "*.proc" -exec chmod 640 {} \;
find /home/ubuntu/bfwk/bin/ -type f -name "*.txt"  -exec chmod 640 {} \;

find /home/ubuntu/bfwk/conf/ -type f -exec chmod 600 {} \;
