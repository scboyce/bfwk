###############################################################################
#
# Program     : comFunctions.sh
#
# Description : Common functions available for use by all shell scripts
#
# Notes       : To use this, just source it at the top of your script.
#
# Date       Developer      Description
# ---------- -------------- --------------------------------------------------
# 2014-11-14 Steve Boyce    Initial release
# 2018-02-12 Steve Boyce    Added SendFiletoHdfs
# 2018-02-28 Steve Boyce    Added RunImpalaSql
#
###############################################################################

NowBlurb() {
   #-- $1 is Program name (no path)
   #-- $2 is String message
   echo `date +"%Y-%m-%d %H:%M:%S"` "${1} ${2}"
}

##############################################################################

Die() {
    local message=$1
    [ -z "$message" ] && message="Died"
    echo `date +"%Y-%m-%d %H:%M:%S"` `basename ${BASH_SOURCE[1]}`" (line ${BASH_LINENO[0]}) ${message}" >&2
    exit 1
}

##############################################################################

AddTrailingLF() {
   #-- $1 file

   #-- Add trailing line feed if missing
   local RetVal
   local lastline
   local File

   RetVal=1
   if [[ -n $1 ]]; then
      File=$1
      if [[ -s ${File} ]]; then
         lastline=$(tail -n 1 ${File}; echo x)
         lastline=${lastline%x}
         if [ "${lastline: -1}" != $'\n' ]; then
            echo "" >> ${File}
            RetVal=$?
         else
            #-- nothing to do
            RetVal=0
         fi
      else
         NowBlurb "Error: $File is unreadable or zero byte." 
         RetVal=1
      fi
   else
      NowBlurb "Error: Missing file." 
      RetVal=1
   fi
   return ${RetVal}
}

##############################################################################

SendFiletoHdfs() {
   local HdfsUser=$1
   local HdfsHost=$2
   local HdfsUserKey=$3
   local SourceFile=$4
   local TargetFile=$5
   local MaxTransferRetrys=$6
   local TransferRetrySleep=$7

   local SourceChkSum=""
   local TransferAttempt=1
   local RetVal=0

   echo
   NowBlurb ${Prog} "Entering comFunctions.sh/SendFiletoHdfs..."
   NowBlurb ${Prog} "HdfsUser: ${HdfsUser}"
   NowBlurb ${Prog} "HdfsHost: ${HdfsHost}"
   NowBlurb ${Prog} "HdfsUserKey: ${HdfsUserKey}"
   NowBlurb ${Prog} "SourceFile: ${SourceFile}"
   NowBlurb ${Prog} "TargetFile: ${TargetFile}"
   NowBlurb ${Prog} "MaxTransferRetrys: ${MaxTransferRetrys}"
   NowBlurb ${Prog} "TransferRetrySleep: ${TransferRetrySleep}"

   if [[ -n "${HdfsUser}" &&
         -n "${HdfsHost}" &&
         -n "${HdfsUserKey}" &&
         -n "${SourceFile}" &&
         -n "${TargetFile}" &&
         -n "${MaxTransferRetrys}" &&
         -n "${TransferRetrySleep}"
      ]]; then

      if [[ -r ${SourceFile} ]]; then

         NowBlurb ${Prog} "Computing md5sum of SourceFile..."
         SourceChkSum=$(cat ${SourceFile} | md5sum)
         NowBlurb ${Prog} "SourceChkSum: ${SourceChkSum}"

         while [ true ]; do
            NowBlurb ${Prog} "-----------------------------"
            NowBlurb ${Prog} "Transfer to HDFS attempt ${TransferAttempt} of ${MaxTransferRetrys}..."

            cat ${SourceFile} | \
               ssh -o StrictHostKeyChecking=no \
                   -i ${HdfsUserKey} \
                   ${HdfsUser}@${HdfsHost} \
                   "hdfs dfs -put -f - ${TargetFile}"

            NowBlurb ${Prog} "Computing md5sum of: ${TargetFile}..."
            TargetChkSum=$(ssh -o StrictHostKeyChecking=no -i ${HdfsUserKey} \
                       ${HdfsUser}@${HdfsHost} \
                       "hdfs dfs -cat ${TargetFile} | md5sum")

            NowBlurb ${Prog} "SourceChkSum: ${SourceChkSum}"
            NowBlurb ${Prog} "TargetChkSum: ${TargetChkSum}"

            if [ "${SourceChkSum}" == "${TargetChkSum}" ]; then
               #-- Checksums match, transfer must have succeeded
               NowBlurb ${Prog} "Transfer succeeded."
               break
            else
               NowBlurb ${Prog} "Transfer ${TransferAttempt} failed."
               if [[ ${TransferAttempt} -ge ${MaxTransferRetrys} ]]; then
                  NowBlurb ${Prog} "Exhausted the number of retrys: ${MaxTransferRetrys}."
                  NowBlurb ${Prog} "Error: Unable to transfer ${SourceFile} to ${HdfsUser}@${HdfsHost}:${TargetFile}"
                  RetVal=1
                  break
               else
                  NowBlurb ${Prog} "Sleeping for ${TransferRetrySleep} seconds..."
                  sleep ${TransferRetrySleep}
               fi
            fi
            TransferAttempt=$[${TransferAttempt}+1]
         done
      else
         NowBlurb ${Prog} "Error: Unable to read ${SourceFile}"
         RetVal=1
      fi
   else
      NowBlurb ${Prog} "Error: missing one or more parameters to SendFiletoHdfs()."
      RetVal=1
   fi
   echo
   return ${RetVal}
}

##############################################################################

RunImpalaSql() {
   local ImpalaHost=$1
   local ImpalaPort=$2
   local ImpalaUser=$3
   local ImpalaUserKey=$4
   local MaxImpalaRetrys=$5
   local ImpalaRetrySleep=$6
   local ImpalaSql=$7

   local Sql
   local RunAttempt=1
   local RetVal=0

   echo
   NowBlurb ${Prog} "Entering comFunctions.sh/RunImpalaSql..."
   NowBlurb ${Prog} "ImpalaHost: ${ImpalaHost}"
   NowBlurb ${Prog} "ImpalaPort: ${ImpalaPort}"
   NowBlurb ${Prog} "ImpalaUser: ${ImpalaUser}"
   NowBlurb ${Prog} "ImpalaUserKey: ${ImpalaUserKey}"
   NowBlurb ${Prog} "MaxImpalaRetrys: ${MaxImpalaRetrys}"
   NowBlurb ${Prog} "ImpalaRetrySleep: ${ImpalaRetrySleep}"
   NowBlurb ${Prog} "ImpalaSql:"
   echo "${ImpalaSql}"
   echo

   if [[ -n "${ImpalaHost}" &&
         -n "${ImpalaPort}" &&
         -n "${ImpalaUser}" &&
         -n "${ImpalaUserKey}" &&
         -n "${MaxImpalaRetrys}" &&
         -n "${ImpalaRetrySleep}" &&
         -n "${ImpalaSql}"
      ]]; then

      while [ true ]; do
         NowBlurb ${Prog} "-----------------------------"
         NowBlurb ${Prog} "Impala Run attempt ${RunAttempt} of ${MaxImpalaRetrys}..."

         ssh -o StrictHostKeyChecking=no \
             -i ${ImpalaUserKey} \
             ${ImpalaUser}@${ImpalaHost} \
             "impala-shell --delimited --print_header -i ${ImpalaHost}:${ImpalaPort} -q \"${ImpalaSql}\""
         RetVal=$?

         if [ ${RetVal} -eq 0 ]; then
            NowBlurb ${Prog} "Impala-shell run succeeded."
            break
         else
            NowBlurb ${Prog} "Impala-shell run attempt ${RunAttempt} failed."
            if [[ ${RunAttempt} -ge ${MaxImpalaRetrys} ]]; then
               NowBlurb ${Prog} "Exhausted the number of retrys: ${MaxImpalaRetrys}."
               NowBlurb ${Prog} "Error: Unable to execute Impala script.}"
               RetVal=1
               break
            else
               NowBlurb ${Prog} "Sleeping for ${ImpalaRetrySleep} seconds..."
               sleep ${ImpalaRetrySleep}
            fi
         fi
         RunAttempt=$[${RunAttempt}+1]
      done
   else
      NowBlurb ${Prog} "Error: missing one or more parameters to RunImpalaSql()."
      RetVal=1
   fi
   echo
   return ${RetVal}
}

##############################################################################
#-- EOF
