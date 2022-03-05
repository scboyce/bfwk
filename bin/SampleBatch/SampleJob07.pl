#!/usr/bin/perl
##############################################################################
#
# Program: SampleJob01.pl
#
# Description : Sample scripted job
#
# Parameters  :  $1 - (C) ParameterFile - Fully qualified parameter file
#
# Notes       : Example script
#
# Date       Developer      Description
# ---------- -------------- --------------------------------------------------
# 2022-02-11 Steve Boyce    Initial release
#
##############################################################################
#-- Standard header code
###############################################################################

use strict;
use File::Basename;
use File::Copy;
use Cwd 'abs_path';
use Sys::Hostname;
use Env;
use JSON qw(decode_json);
use Text::CSV;
use Data::Dumper;

use FindBin;
use lib "$FindBin::Bin/..";
use comFunctions;

#-- Debuffer output
$| = 1;

PrintInfo("Starting...");

my $BatchConfFile = $ARGV[0];
PrintInfo("BatchConfFile: $BatchConfFile");

#-- GetBatchParameter($BatchParamHashRef, "<parameter>", <required>, <showit>);
#--    Required
#--       0 - OK if missing
#--       1 - Die if missing
#--    ShowIt
#--       0 - Don't show it at all
#--       1 - Disply value
#--       2 - Display masked value

my $BatchParamHashRef = GetBatchParameters($BatchConfFile);
my $BatchName         = GetBatchParameter($BatchParamHashRef, "BatchName",         1, 1);
my $BinFileDirectory  = GetBatchParameter($BatchParamHashRef, "BinFileDirectory",  1, 1);
my $DataFileDirectory = GetBatchParameter($BatchParamHashRef, "DataFileDirectory", 1, 1);
my $WorkFileDirectory = GetBatchParameter($BatchParamHashRef, "WorkFileDirectory", 1, 1);

my $HostName = hostname;
my $LinuxUserName = getlogin;
my $ConfigFileDirectory = abs_path(dirname($BatchConfFile));

PrintInfo("HostName: $HostName");
PrintInfo("LinuxUserName: $LinuxUserName");
PrintInfo("ConfigFileDirectory: $ConfigFileDirectory");

###############################################################################
#-- Main
###############################################################################
PrintInfo("Main...");

PrintInfo("RUN_BY_CRON: ".$ENV{'RUN_BY_CRON'});
PrintInfo("HOME: ".$ENV{'HOME'});
PrintInfo("PATH: ".$ENV{'PATH'});

for (my $i=1; $i <= 3; $i++) {
    PrintInfo("Counter: $i - Date/Time: ". NowDate());
    sleep 1;

}
PrintInfo("Done.");
