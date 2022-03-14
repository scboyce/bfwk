#!/bin/bash
##############################################################################
#
# Program: DeployGitHubVersion.sh
#
# Description: Utility script to deploy GitHub code to this environment
#                 - Can deploy the head of master
#                 - Can deploy a specific branch or tag
#
# === Modification History ===================================================
# Date       Author          Comments
# ---------- --------------- -------------------------------------------------
# 2022-03-10 Steve Boyce     Created.
#
##############################################################################

#-- Process options if any
Usage="Usage $(basename $0).sh [-b<branch> -d -h] <deployer>\n"

while getopts ":b:dvh" Option
do
   case $Option in
      b )  Opt_b="TRUE"
           Opt_b_arg=$OPTARG
           ;;
      d )  Opt_d="TRUE"
           ;;
      h )  Opt_h="TRUE"
           ;;
      \?)  echo "Error: unrecognized option."
           echo -e $Usage
           exit 1
           ;;
      :)   "Option -$OPTARG requires an argument."
           exit 1
           ;;
   esac
done
shift $(($OPTIND - 1))

if [[ $Opt_h = "TRUE" ]]; then
   echo
   echo -e $Usage
   cat <<EOH
Where: 
      <deployer>  Name of person running this script (for logging purposes)
                  Required.  Type in your initials or name.  Honor system.

      -b<Branch>  Deploy named Branch or Tag
                  Optional.  Will deploy the head of master if not specificed.

      -d          Dry Run.  Don't actually deploy, instead show what will be deployed.

      -h          This Help

EOH
exit 1
fi

#-------------------------------------------------------------
AreYouLoggedIn () {
   read -r -p "${1:-Are you logged in as the Linux Batch Framework batch user? [y/n]} " response
   case $response in
      [yY][eE][sS]|[yY])
         true
         ;;
      *)
         false
         ;;
   esac
}

#-------------------------------------------------------------
AreYouSure () {
   read -r -p "${1:-Are you sure? [y/n]} " response
   case $response in
      [yY][eE][sS]|[yY]) 
         true
         ;;
      *)
         false
         ;;
   esac
}

#-------------------------------------------------------------
AreYouReallySure () {
   read -r -p "${1:-Are you really sure? [sure/n]} " response
   case $response in
      [sS][uU][rR][eE])
         true
         ;;
      *)
         false
         ;;
   esac
}

#-------------------------------------------------------------
#-- Main

#-- Make sure we know who is doing this.  Honor system
if [[ -n $1 ]]; then
   deployer="$1"
else
   echo
   echo "Error: <deployer> is required."
   echo
   echo -e $Usage
   exit 1
fi

TimeStamp=$(date +"%Y-%m-%dT%H-%M-%S")
Hostname=$(echo $HOSTNAME | cut -d"." -f1)
CurrentDirectory=$(dirname "$0")
BfDirectory=$(realpath ${CurrentDirectory}/..)
BfBinDirectory="${BfDirectory}/bin"
BfConfigDirectory="${BfDirectory}/conf"
DeployLogDirectory=$(realpath ${BfDirectory}/../deploy)
TempDirectory="/var/tmp"
TempGitDirectory="${TempDirectory}/${TimeStamp}"

echo "Deployer: ${deployer}"
echo "TimeStamp: ${TimeStamp}"
echo "Hostname: ${Hostname}"
echo "BfDirectory: ${BfDirectory}"
echo "BfBinDirectory: ${BfBinDirectory}"
echo "BfConfigDirectory: ${BfConfigDirectory}"
echo "DeployLogDirectory: ${DeployLogDirectory}"
echo "TempDirectory: ${TempDirectory}"
echo "TempGitDirectory: ${TempGitDirectory}"

#-- Source config file
. ${BfConfigDirectory}/DeployGitHubVersion.cfg

echo "GitRemote: ${GitRemote}"

if [[ -z "${GitRemote}" ]]; then
   echo "Error: Unable to determine Git remote."
fi

echo
if [[ $Opt_d = "TRUE" ]]; then
   echo "WARNING: DryRun mode IS enabled."
else
   echo "WARNING: DryRun mode IS NOT enabled."
fi

echo
if [[ $Opt_b = "TRUE" ]]; then
   echo "Deploying Branch/Tag: ${Opt_b_arg}"
else
   echo "Deploying HEAD of master branch"
fi
echo

AreYouSure
if [[ $? -eq 0 ]]; then
   AreYouReallySure
   if [[ $? -eq 0 ]]; then
      echo
      echo "Deploying..."
      echo
   else
      echo "Canceled"
      exit 1
   fi
else
   echo "Canceled"
   exit 1
fi

if [[ $Opt_b = "TRUE" ]]; then
   git clone --depth 1 --branch ${Opt_b_arg} ${GitRemote} ${TempGitDirectory}
else
   git clone --depth 1 ${GitRemote} ${TempGitDirectory}
fi

echo

if [[ $Opt_d = "TRUE" ]]; then
   #-- Dry run
   echo "RSync DryRun mode..."
   echo

   rsync --verbose \
         --dry-run \
         --exclude="__pycache__" \
         --dirs \
         --itemize-changes \
         --perms \
         --recursive \
         --checksum \
         --delete \
         ${TempGitDirectory}/bin \
         ${BfDirectory}
else
   #-- Real run
   echo "Starting RSync..."
   echo

   rsync --verbose \
         --exclude="__pycache__" \
         --dirs \
         --itemize-changes \
         --perms \
         --recursive \
         --checksum \
         --delete \
         ${TempGitDirectory}/bin \
         ${BfDirectory}
fi

echo "Done."
