#!/bin/bash
###############################################################################
#
# Program     : SampleJob01.sh
#
# Description : Sample scripted job
#
# Parameters  :  $1 - (C) ParameterFile - Fully qualified parameter file
#
# Notes       : Example script
#
# Date       Developer      Description
# ---------- -------------- --------------------------------------------------
# 2016-12-16 Steve Boyce    Initial release
#
###############################################################################
#-- Standard header code
###############################################################################
. $(dirname "$0")/../comFunctions.sh
Prog=$(basename ${0})
NowBlurb ${Prog} "Initializing..."
ConfigFile="${1}"
NowBlurb ${Prog} "ConfigFile: ${ConfigFile}"
NowBlurb ${Prog} "BatchNumber: ${BatchNumber}"
if [[ ${#} -ne 1 ]]; then
   RetVal=1
   NowBlurb ${Prog} "Error:  Wrong number of parameters.  Expecting ConfigFile."
   NowBlurb ${Prog} "Exiting with return value of: ${RetVal}"
   exit ${RetVal}
fi

NowBlurb ${Prog} "Sourcing ConfigFile..."
. $ConfigFile
if [[ $? -ne 0 ]]; then
   RetVal=1
   NowBlurb ${Prog} "Error: Unable to source ConfigFile."
   NowBlurb ${Prog} "Exiting with return value of: ${RetVal}"
   exit ${RetVal}
fi
NowBlurb ${Prog} "Done."

###############################################################################
#-- Main
###############################################################################
NowBlurb ${Prog} "Main..."
NowBlurb ${Prog} "PID: $$"
NowBlurb ${Prog} "HOME: $HOME"
NowBlurb ${Prog} "RUN_BY_CRON: $RUN_BY_CRON"
NowBlurb ${Prog} "PATH: $PATH"
NowBlurb ${Prog} "umask: $(umask)"

Counter=1
Max=3
NowBlurb ${Prog} "Counter: $Counter"
NowBlurb ${Prog} "Max: $Max"

while [[ ${Counter} -le ${Max} ]]
do
   NowBlurb ${Prog} "Counter: ${Counter} - Date/Time: `date '+%Y-%m-%d %H:%M:%S'`"
   Counter=$((${Counter} + 1))
   sleep 1
done

RetVal=0
NowBlurb ${Prog} "Complete, exiting with status: ${RetVal}"
exit ${RetVal}
