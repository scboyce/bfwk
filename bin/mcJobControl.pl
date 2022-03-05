#!/usr/bin/perl -w
##############################################################################
#
# Program: mcJobControl.pl
#
# Description: Runs a batch of scripted jobs.
#              Type mcJobControl.pl -h for options and parameters.
#
# Features:
#    BatchNumber Generation (unique based on time)
#    Run Numbers
#    Process Date
#    Batch Message Log
#    Batch Audit Log
#    Batch History Log
#    Batch Configuration File
#    Process List File
#    Serial/Parallel Execution
#    Process Dependencies
#    Milestone Processes
#    Process Message Logging
#    Process Audit Log 
#    Process Error Trapping
#    Process Validation
#    Predecessor Validation
#    Deadly Embrace Detection
#    LastSuccessfulBatchNumber
#    LastSuccessfulRunNumber
#    LastSuccessfulProcessDate
#    Resurrect Mode
#    DebugMode Option
#    BatchType Option
#    BatchAlias Option
#    TestMode Option
#    ProcessDate Option
#    Pause Flag
#    Stop Flag
#    Job Alerting
#    Audit Table Updates
#    Configurable number of BatchMessageLog files
#    Configurable number of ProcessMessageLog files
#    Processes can be commented out in the ProcessList
#    Textual comments can be added in the ProcessLists
#    Concurrent job throttling
#
# Internals:
#
#    -- Array list of jobs and predecessor jobs to used to see all other lists
#    -- This list is initialized from <BatchName>.proc
#    @ProcessList - 2 Dim Array
#                   Structure
#                   ----------------
#                   ProcessName
#                   Predecessors
#
#    -- Array list of commented out jobs
#    -- This list is initialized from <BatchName>.proc
#    @CommentedOutProcessList - 1 Dim Array
#                   Structure
#                   ----------------
#                   ProcessName
#
#    -- Hash list to keep track of job runtime statuses
#    %ProcessStatusList - Hash of 1 Dim Arrays
#                   Structure
#                   ----------------
#                   ProcessName => [ RunNumber, NaturalOrder, RunOrder, oProcess, PID, Status, StartTime, EndTime ]
#
#    -- Hash of job predecessrors by job
#    %ProcessPredecessorList - Hash of 1 Dim Arrays
#                   Structure
#                   ----------------
#                   ProcessName => [ PredecessorList ]
#
#    -- Job command and process message log
#    %ProcessFileNameList - Hash of 1 Dim Arrays
#                   Structure
#                   ----------------
#                   ProcessName => [ ProcessCommand, ProcessMessageLog ]
#
#    -- Hash of Test jobs and fake run time counter.
#    -- This hash is used to fake a job run for Test jobs.
#    -- When a Test job starts it get set to 0.
#    -- When the job statuses are checked the counter is bumped and when the
#    -- counter grows larger than 1, the Test job finishes sucessfully.
#    %TestJobCounterList - Hash
#                   Structure
#                   ----------------
#                   TestJobName => LoopCounter
#
#    -- Hash of Milestone jobs and fake run time counter.
#    -- This hash is used to fake a job run for Milestone jobs.
#    -- When a Milestone job starts it get set to 0.
#    -- When the job statuses are checked the counter is bumped and when the
#    -- counter grows larger than 1, the Milestone job finishes sucessfully.
#    %MilestoneJobCounterList - Hash
#                   Structure
#                   ----------------
#                   MilestoneName => LoopCounter
#
#    -- BatchHistory log file structure:
#    BatchNumber|RunNumber|BatchName|ProcessDate|BatchStatus|BatchStartTime|BatchEndTime|BatchType|BatchAlias
#
#    -- BatchAudit log file structure:
#    BatchNumber|RunNumber|BatchName|ProcessDate|BatchStatus|BatchStartTime|BatchEndTime|BatchType|BatchAlias
#
#    -- ProcessAudit log file structure:
#    BatchNumber|RunNumber|ProcessName|ProcessStatus|ProcessStartTime|ProcessEndTime
#
#
# === Modification History ===================================================
# Date       Author          Comments
# ---------- --------------- ------------------------------------------------------
# 2014-10-27 Steve Boyce     Initial Implementation
# 2017-06-08 Steve Boyce     Batches no longer auto-resurrect
#                            .proc files are now expected to be in the batch directory
# 2017-07-17 Steve Boyce     Operational Metadata feature now works with MySQL
# 2022-02-10 Steve Boyce     Figured out how to include comFunctions without hardcoding
#                            Added setting BatchType to AUTO when RUN_BY_CRON=TRUE
#
##############################################################################

use strict;
use DBI;
use Getopt::Std;
use File::Basename;
use Proc::Simple;
use File::Find;
use File::Copy;
use File::Path;
use Date::Pcalc qw(Today_and_Now
                   Delta_DHMS);
use Fcntl qw(:flock);
use Email::Stuffer;
use Net::Domain qw(domainname);

use FindBin;
use lib "$FindBin::Bin";
use comFunctions;

#-- Debuffer output
$| = 1;

#-- Declare all getopt vars
use vars qw($opt_a
            $opt_b
            $opt_o
            $opt_u
            $opt_s
            $opt_e
            $opt_d
            $opt_r 
            $opt_p
            $opt_t
            $opt_x
            $opt_h);

#-- Declare all global constant variables
use vars qw($True
            $False
            $IsUnix
            $Slash
           );

#-- Declare all global variables
use vars qw($BatchNumber
            $RunNumber
            $BatchStartTime
            $LastSuccessfulBatchNumber
            $LastSuccessfulRunNumber
            $ProcessDate
            $LastSuccessfulProcessDate
            $NowEndTime
            $ConfigurationFile
            $ProcessListFile
            $BatchType
            $BatchAlias
            $Heartbeat
            $TestMode
            $DebugMode
            $dbh
            $FirstUpdateOfBatchAuditTable

            $ApplicationName
            $BatchName
            $JobPollInterval
            $MaxParallelJobs
            $MaxArchivedLogs
            $PerformAuditTableUpdates
            $AuditTableUpdateInterval
            $AuditTableCriticality
            $BfConnectString
            $BfUserId
            $BfUserPassword
            $BfBinFileDirectory
            $BfLogFileDirectory
            $BfLockFileDirectory
            $BinFileDirectory
            $LogFileDirectory
            $PollFileDirectory
            $WorkFileDirectory
            $SendFailureMessage
            $AlertEMailList
            $DomainName
            $HostUserName

            $BatchMessageLogFile
            $BatchAuditLogFile
            $BatchHistoryLogFile
            $ProcessAuditLogFile
            @ProcessList
            @CommentedOutProcessList
            %ProcessStatusList
            %ProcessPredecessorList
            %TestJobCounterList
            %MilestoneJobCounterList
            %ProcessFileNameList
            $RunOrder
            $OverallProcessRunning
            $OverallProcessWaiting
            $OverallProcessFailed
            $BatchAuditStatus
            $AuditTableErrorLatch
            $ResurrectMode
           );

##############################################################################
sub ShowBlurb
{
print <<ENDOFBLURB;

Syntax: mcJobControl.pl <ConfigurationFile>

Description: Runs a batch of scripted jobs in dependency order defined in process list.

Parameters:
   ConfigurationFile - Fully qualified name of batch configuration file

Options: 
   -a<alias>  - BatchAlias - Alternate name for a batch.
                Optional, defaults to BatchName.
                Format = "<Alias>" no spaces.

   -b<number> - BatchNumber - Batch number associated with this batch run
                Optional, defaults to next unique number
                Format = "YYYYMMDDHH24MISS"

   -s<number> - StartingMileStone - Execute jobs starting with (including) this milestone job
                Optional, defaults to jobs configured to run first
                Format = "<number>"

   -e<number> - EndingMileStone - Execute jobs up to (not including) this milestone job
                Optional, defaults to end of job stream
                Format = "<number>"

   -d         - DebugMode flag
                Optional, defaults to not displaying debug level messages

   -r         - ResurrectMode flag - Resurrect last Batch
                Can also be triggered by touching RES.flg file in poll directory
                Optional, defaults to NOT resurrecting

   -p<date>   - ProcessDate - Reference date used for date range processing
                Optional, defaults to system date
                Format = "YYYY-MM-DD HH24:MI:SS"

   -t<type>   - BatchType - Type of batch. Automatic or Manual
                Optional, defaults to Manual, set to AUTO when called from cron
                Format = "AUTO"|"MANUAL"
                Note: Setting RUN_BY_CRON="TRUE" in crontab will set BatchType to "AUTO".

   -x         - TestMode - Runs dummy (sleep 1) jobs instead of the real jobs
                Can also be triggered by touching TEST.flg file in poll directory
                Optional, default to running job listed in the ProcessList file

   -h         - This help

Configuration File Parameters:
   ApplicationName           - Name of application
   BatchName                 - Name of batch
   JobPollInterval           - Number of seconds to wait between passed of list of jobs to launch and poll
                               Default is 2
   MaxParallelJobs           - Maximum number of jobs that can run at the same time
                               Default is 0 (unlimited)
   MaxArchivedLogs           - Number of rolling historical logs to retain
                               Default is 3
                               Unlimited is 0

   PerformAuditTableUpdates  - Keep operational metadata tables updated
                               Y/N
   AuditTableUpdateInterval  - Number of seconds to wait between updates of operational metadata
   AuditTableCriticality     - Defines severity of operational metadata updates
                               WARN  = Scripted Job Control produces warning only
                               ERROR = Scripted Job Control terminates with error
   BfConnectString           - Database where operational metadata tables reside
   BfUserId                  - Operational metadata user id
   BfUserPassword            - Operational metadata user password

   BfBinFileDirectory        - Location of common binary/script file directory
   BfLogFileDirectory        - Location of common log file directory
   BfLockFileDirectory       - Location of common lock file directory
   BinFileDirectory          - Location of binar/script file directory
   LogFileDirectory          - Location of log file directory
   PollFileDirectory         - Location of poll file directory
   WorkFileDirectory         - Location of work file directory
   SendFailureMessage        - Send Process failure email alerts
                               Y/N
   AlertEMailList            - Comma separted list of email addresses to send Process email alerts

Poll Directory Signal Files:
   PAUSE.flg     - Scripted Job Control will stop launching new processes and continue to
                   monitor running processes.  Remove file to un-pause.
   RES.flg       - Scripted Job Control will attempt to resurrect previously failed batch
   STOP.flg      - Scripted Job control will stop launching new processes and continue to
                   monitor running processes.  Batch will exit with failure when last running
                   job ends regardless of process exit status.
   TEST.flg      - Scripted Job Control will sleep for 1 second in place of each process.

ExitCodes:
    0 - All jobs successful
    1 - Initialization error, shutdown before any jobs started
    2 - Critical Job Control error, PANIC, shutdown immediate, jobs may still be running
    3 - Reserved
    4 - Reserved
    5 - STOP flag detected, shutdown after all jobs ended
    6 - One or more jobs failed, shutdown after all jobs ended, no waiting jobs launched

ENDOFBLURB
}

###################################################################
sub ElapsedSecondsFromSeedDate
{
   my $Dd;
   my $Dh;
   my $Dm;
   my $Ds;

   my ($Year1, $Month1, $Day1, $Hour1, $Min1, $Sec1) = ("1990", "01", "01", "00", "00", "00");
   my ($Year2, $Month2, $Day2, $Hour2, $Min2, $Sec2) = Today_and_Now();

   ($Dd,$Dh,$Dm,$Ds) = Delta_DHMS($Year1, $Month1, $Day1, $Hour1, $Min1, $Sec1,
                                  $Year2, $Month2, $Day2, $Hour2, $Min2, $Sec2);

   my $TotalSeconds = $Ds + ($Dm * 60) + ($Dh * 3600) + ($Dd * 86400);

   return $TotalSeconds;
}

##############################################################################
sub PrintInfoToBatchMessageLog
{
   my ($MessageLine) = @_;

   if ( open(fhBatchMessageLogFile, ">>$BatchMessageLogFile") ) {
      print fhBatchMessageLogFile NowDate(""), " ", basename($0), " $MessageLine\n";
      close fhBatchMessageLogFile;
      PrintInfo("--> $MessageLine");
   }
   else {
      PrintInfo("Error: Can't write to BatchMessageLogFile: $BatchMessageLogFile");
   }
}

##############################################################################
sub AllPredecessorsComplete
{
   my ($ProcessName) = @_;

   my $RetVal = $True;
   my $x = 0;

   if ( $ProcessPredecessorList{$ProcessName} ) {
      #-- Have predecessors
      #-- Spin through list of predecessors for this job and see if they are complete
      for ( $x = 0; $ProcessPredecessorList{$ProcessName}[$x]; $x++ ) {
         if ( $ProcessStatusList{$ProcessPredecessorList{$ProcessName}[$x]}[5] ne "SUCCESSFUL" ) {
            $RetVal = $False;
         }
      }
   }
   return $RetVal;
}

##############################################################################
sub ProcessIsReadyToLaunch
{
   my ($ProcessName) = @_;

   my $RetVal = $False;

   #-- Job must be waiting to be eligeble for launching
   if ( $ProcessStatusList{$ProcessName}[5] eq "WAITING" ) {
      #-- Only allow new jobs to start as long as there are no other failed jobs
      if ( ! $OverallProcessFailed ) {
         #-- All predecessor jobs must be completed
         if ( AllPredecessorsComplete($ProcessName) ) {
            #-- OK to launch
            $RetVal = $True;
         }
      }
   }
   return $RetVal;
}

##############################################################################
sub WriteBatchHistoryLog
{
   my $RetVal = $False;

   if ( open(fhBatchHistoryLogFile, ">>$BatchHistoryLogFile") ) {
      if ( open(fhBatchAuditLogFile, "<$BatchAuditLogFile") ) {
         while ( <fhBatchAuditLogFile> ) {
            $RetVal = $True;
            print fhBatchHistoryLogFile $_;
         }
         close fhBatchAuditLogFile;
      }
      else {
         PrintInfoToBatchMessageLog("Error: Unable to read batch audit log file: $BatchAuditLogFile");
      }
      close fhBatchHistoryLogFile;
   }
   else {
      PrintInfoToBatchMessageLog("Error: Unable to write batch history log file: $BatchHistoryLogFile");
   }
   return $RetVal;
}

##############################################################################
sub WriteBatchAuditLog
{
   my ($BatchAuditStatus) = @_;
   my $RetVal = $True;
   my $BatchEndTime = "";

   if ( $DebugMode ) {
      PrintInfo("*Debug* About to write BatchAuditLogFile - BatchAuditStatus: $BatchAuditStatus...");
   }
   if ( $BatchAuditStatus eq "SUCCESSFUL" || $BatchAuditStatus eq "FAILED" ) {
      #-- Final update
      $BatchEndTime = $NowEndTime;
      PrintInfoToBatchMessageLog("BatchEndTime: ${BatchEndTime}");
   }

   if ( open(fhBatchAuditLogFile, ">$BatchAuditLogFile") ) {
      #-- File opened
      print fhBatchAuditLogFile "$BatchNumber",
                                "|$RunNumber",
                                "|$BatchName",
                                "|$ProcessDate",
                                "|$BatchAuditStatus",
                                "|$BatchStartTime",
                                "|$BatchEndTime",
                                "|$BatchType",
                                "|$BatchAlias",
                                "\n";
      close fhBatchAuditLogFile;
   }
   else {
      $RetVal = $False;
      PrintInfoToBatchMessageLog("Error: Unable to write batch audit log file: $BatchAuditLogFile");
   }
   return $RetVal;
}

##############################################################################
sub WriteProcessAuditLog
{
   my $RetVal = $True;
   my $x = 0;
   my $ProcessName = "";

   if ( $DebugMode ) {
      PrintInfo("*Debug* About to write ProcessAuditLogFile...");
   }
   if ( open(fhProcessAuditLogFile, ">$ProcessAuditLogFile") ) {
      #-- File opened
      for ($x = 0; $ProcessList[$x][0]; $x++) {
         $ProcessName = $ProcessList[$x][0];
         print fhProcessAuditLogFile "$BatchNumber",
                                     "|$ProcessStatusList{$ProcessName}[0]",
                                     "|$ProcessName",
                                     "|$ProcessStatusList{$ProcessName}[5]",
                                     "|$ProcessStatusList{$ProcessName}[6]",
                                     "|$ProcessStatusList{$ProcessName}[7]",
                                     "\n";
      }
      close fhProcessAuditLogFile;
   }
   else {
      $RetVal = $False;
      PrintInfoToBatchMessageLog("Error: Unable to write process audit log file: $ProcessAuditLogFile");
   }
   return $RetVal;
}

##############################################################################
sub WriteAuditLogs
{
   my ($BatchAuditStatus) = @_;
   my $RetVal = $False;

   if ( WriteBatchAuditLog($BatchAuditStatus) ) {
      if ( WriteProcessAuditLog() ) {
         $RetVal = $True;
      }
   }
   return $RetVal;
}

##############################################################################
sub UpdateBatchAuditTable
{
   my ($BatchAuditStatus) = @_;
   my $RetVal = $False;
   my $BatchEndTime = undef;
   my $Heartbeat = NowDate();
   my $INSERT = "INSERT INTO etl_batch_audit 
                      (system_name, batch_number, run_number, batch_name, process_date,
                       batch_status, batch_start_time, batch_end_time, batch_type, batch_alias, heartbeat)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
   my $UPDATE = "UPDATE etl_batch_audit
                    SET batch_status = ?, 
                        batch_end_time = ?,
                        heartbeat = ?
                  WHERE system_name = ? AND batch_number = ? AND run_number = ?";
   my $sth;

   if ( $DebugMode ) {
      PrintInfo("*Debug* About to update ETL_BATCH_AUDIT - BatchAuditStatus: $BatchAuditStatus...");
   }
   if ( $BatchAuditStatus eq "SUCCESSFUL" || $BatchAuditStatus eq "FAILED" ) {
      #-- Final update
      $BatchEndTime = $NowEndTime;
   }

   if ( $FirstUpdateOfBatchAuditTable ) {
      $FirstUpdateOfBatchAuditTable = $False;
      $sth = $dbh->prepare($INSERT);
      $sth->execute($ApplicationName, $BatchNumber, $RunNumber, $BatchName, $ProcessDate, $BatchAuditStatus, 
                   ($BatchStartTime ? $BatchStartTime : undef), ($BatchEndTime ? $BatchEndTime : undef),
                   $BatchType, $BatchAlias, $Heartbeat);
      if ( ! $sth->err ) {
         $dbh->commit;
         $RetVal = $True;
      }
      else {
         PrintInfoToBatchMessageLog("Error: $DBI::errstr");
      }
   }
   else {
      #-- Update table
      $sth = $dbh->prepare($UPDATE);
      $sth->execute($BatchAuditStatus, ($BatchEndTime ? $BatchEndTime : undef), $Heartbeat, $ApplicationName, $BatchNumber, $RunNumber);
      if ( ! $sth->err ) {
         $dbh->commit;
         $RetVal = $True;
      }
      else {
         PrintInfoToBatchMessageLog("Error: $DBI::errstr");
      }
   }
   return $RetVal;
}

##############################################################################
sub NumberOfProcessAuditTableRecords
{
   my ($ProcessName, $ProcessRunNumber) = @_;
   my $RetVal = -1;
   my $Count;
   my $SQL;
   my $sth;

   $SQL = "SELECT count(*)
             FROM etl_process_audit
            WHERE system_name = ?
              AND batch_number = ?
              AND process_name = ?
              AND run_number = ?";

   $sth = $dbh->prepare($SQL);
   $sth->execute($ApplicationName, $BatchNumber, $ProcessName, $ProcessRunNumber);
   $sth->bind_columns( \( $Count ) );

   $sth->fetch;
   if ( ! $sth->err ) {
      #-- Count = 0 - No records, OK
      #-- Count = 1 - 1 record, OK
      #-- Count > 1 - Many records, problem
      #-- Count = -1 - Error issuing SQL
      if ( defined($Count) ) {
         $RetVal = $Count;
      }
   }
   else {
      PrintInfoToBatchMessageLog("Error: $DBI::errstr");
   }
   return $RetVal;
}

##############################################################################
sub UpdateProcessAuditTable
{
   my $RetVal = $False;
   my $x = 0;
   my $ProcessRunNumber;
   my $ProcessName;
   my $ProcessStatus;
   my $ProcessStartTime;
   my $ProcessEndTime;
   my $NumberOfExistingRecords;

   my $INSERT = "INSERT INTO etl_process_audit
                      (system_name, batch_number, process_name, run_number, batch_name,
                       process_status, process_start_time, process_end_time)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
   my $UPDATE = "UPDATE etl_process_audit
                    SET process_status = ?,
                        process_start_time = ?,
                        process_end_time = ?
                  WHERE system_name = ? and batch_number = ? and process_name = ? and run_number = ?";
   my $sth;

   if ( $DebugMode ) {
      PrintInfo("*Debug* About to update ETL_PROCESS_AUDIT...");
   }

   for ($x = 0; $ProcessList[$x][0]; $x++) {
      $ProcessName = $ProcessList[$x][0];
      $ProcessRunNumber = $ProcessStatusList{$ProcessName}[0];
      $ProcessStatus = $ProcessStatusList{$ProcessName}[5];
      $ProcessStartTime = $ProcessStatusList{$ProcessName}[6];
      $ProcessEndTime = defined $ProcessStatusList{$ProcessName}[7]? $ProcessStatusList{$ProcessName}[7]: undef;

      $NumberOfExistingRecords = NumberOfProcessAuditTableRecords($ProcessName, $ProcessRunNumber);
      if ( $NumberOfExistingRecords > -1 ) {
         if ( $NumberOfExistingRecords == 0 ) {
            #-- Insert it
            $sth = $dbh->prepare($INSERT);
            $sth->execute($ApplicationName, $BatchNumber, $ProcessName, $ProcessRunNumber, $BatchName, $ProcessStatus,
                          ($ProcessStartTime ? $ProcessStartTime : undef), ($ProcessEndTime ? $ProcessEndTime : undef)
                         );
            if ( ! $sth->err ) {
               $dbh->commit;
               $RetVal = $True;
            }
            else {
               PrintInfoToBatchMessageLog("Error: $DBI::errstr");
            }
         }
         else {
            if ( $NumberOfExistingRecords == 1 ) {
               #-- Update it
               $sth = $dbh->prepare($UPDATE);
               $sth->execute($ProcessStatus, ($ProcessStartTime ? $ProcessStartTime : undef), ($ProcessEndTime ? $ProcessEndTime : undef),
                             $ApplicationName, $BatchNumber, $ProcessName, $ProcessRunNumber);
               if ( ! $sth->err ) {
                  $dbh->commit;
                  $RetVal = $True;
               }
               else {
                  PrintInfoToBatchMessageLog("Error: $DBI::errstr");
               }
            }
            else {
               #-- Error too many ETL_PROCESS_AUDIT records
               PrintInfoToBatchMessageLog("Error: Too many records found in ETL_PROCESS_AUDIT for BatchNumber: $BatchNumber RunNumber: $RunNumber ProcessName: $ProcessName.");
               last;
            }
         }
      }
      else {
         #-- Error selecting from ETL_PROCESS_AUDIT
         PrintInfoToBatchMessageLog("Error: Unable to select from ETL_PROCESS_AUDIT.");
         last;
      }


      #print fhProcessAuditLogFile "$BatchNumber",
      #                            "|$ProcessStatusList{$ProcessName}[0]",
      #                            "|$ProcessName",
      #                            "|$ProcessStatusList{$ProcessName}[5]",
      #                            "|$ProcessStatusList{$ProcessName}[6]",
      #                            "|$ProcessStatusList{$ProcessName}[7]\n";
   }

   return $RetVal;
}

##############################################################################
sub UpdateAuditTables
{
   my ($BatchAuditStatus) = @_;
   my $RetVal = $False;

   if ( $AuditTableErrorLatch ) {
      #-- Already had one error attempting to update tables, don't try again
      $RetVal = $True;
   }
   else {
      if ( UpdateBatchAuditTable($BatchAuditStatus) ) {
         if ( UpdateProcessAuditTable() ) {
            $RetVal = $True;
         }
      }
      if ( ! $RetVal ) {
         #-- Unable to update Audit Tables
         if ( $AuditTableCriticality eq "WARN" ) {
            #-- Errors are not critical, just warn
            PrintInfoToBatchMessageLog("WARNING: Unable to update Audit Tables.");
            PrintInfoToBatchMessageLog("         AuditTableCriticality is set to WARN.");
            PrintInfoToBatchMessageLog("         No more attempts will be made to update Audit Tables.");
            PrintInfoToBatchMessageLog("         Touch RETRY.flg in Poll directory to re-enable.");
            $RetVal = $True;
            $AuditTableErrorLatch = $True;
         }
      }
   }
   return $RetVal;
}

##############################################################################
sub SendJobStatusMessage
{
   my ($ProcessName) = @_;
   my $RetVal = 0;
   my $MessageSubject = "";
   my $MessageBody = "";
   my $ProcessMessageLog = "$ProcessFileNameList{$ProcessName}[1]";

   $MessageSubject = "[ALERT] $ProcessName failed";
   $MessageBody = "";
   $MessageBody .= "Application: $ApplicationName\n";
   $MessageBody .= "Batch: $BatchName\n";
   $MessageBody .= "User: $HostUserName\n";
   $MessageBody .= "Host: $DomainName\n";
   $MessageBody .= "Job: $BinFileDirectory/$ProcessName\n";
   $MessageBody .= "Job Log: $ProcessMessageLog\n\n";
   $MessageBody .= "See attached job log for details.\n";

   if ( "$SendFailureMessage" eq "Y" ) {
      if ( "$AlertEMailList" ) {
         PrintInfoToBatchMessageLog("Sending email alert message...");

         Email::Stuffer->to("$AlertEMailList")
                       ->from("$HostUserName\@$DomainName")
                       ->subject("$MessageSubject")
                       ->text_body("$MessageBody")
                       ->attach_file("$ProcessMessageLog")
                       ->send_or_die;
      }
   }
   return $RetVal;
}

##############################################################################
sub ProcessLoop
{
   my $RetVal = 0;
   my $JobControlFailed = $False;
   my $KeepLooping = $True;
   my $LoopCounter = 0;
   my $RunningProcessesCounter = 0;
   my $LastLoopingPoint = 0;
   my $LastAuditPoint = 0;
   my $CurrLoopingPoint = 0;
   my $x = 0;
   my $ProcessName = "";
   my $ProcessCommand = "";
   my $oProcess;
   my $ProcessPollStatus;
   my $ProcessExitStatus;

   my $PauseFile ="$PollFileDirectory".$Slash."PAUSE.flg";
   my $StopFile  ="$PollFileDirectory".$Slash."STOP.flg";
   my $RetryFile ="$PollFileDirectory".$Slash."RETRY.flg";
   my $TestFile  ="$PollFileDirectory".$Slash."TEST.flg";
   my $PauseMode=0;
   my $StopMode=0;

   #-- Look for TEST poll flag
   if ( -r $TestFile ) {
      #-- Test file found
      $opt_x = $True;
      PrintInfoToBatchMessageLog("*** TEST.flg detected.  Triggering Test Mode...");
   }

   #-- ======================== Top of Loop ==================================
   while ( $KeepLooping ) {
      $LoopCounter++;
      $CurrLoopingPoint = ElapsedSecondsFromSeedDate();

      #-- Look for PAUSE poll flag
      if ( -r $PauseFile ) {
         #-- Pause file found
         $PauseMode++;
         if ( $PauseMode == 1 ) {
            PrintInfoToBatchMessageLog("*** PAUSE.flg detected.  Pausing job control...");
         }
      }
      else {
         #-- No PAUSE file
         if ( $PauseMode ) {
            #-- Must have just deleted it!
            $PauseMode=0;
            PrintInfoToBatchMessageLog("PAUSE.flg removed.  Resuming job control.");
         }
      }

      #-- Look for STOP poll flag
      if ( -r $StopFile ) {
         #-- Stop file found
         $StopMode++;
         if ( $StopMode == 1 ) {
            PrintInfoToBatchMessageLog("*** STOP.flg detected.  Stopping job control after running jobs finish...");
         }
      }
      else {
         #-- No Stop file
         if ( $StopMode ) {
            #-- Must have just deleted it!
            $StopMode=0;
            if ( $PauseMode ) {
               PrintInfoToBatchMessageLog("STOP.flg removed.");
               PrintInfoToBatchMessageLog("*** PAUSE.flg detected.  Pausing job control...");
            }
            else {
               PrintInfoToBatchMessageLog("STOP.flg removed.  Resuming job control.");
            }
         }
      }

      #-- Look for RETRY poll flag
      if ( -r $RetryFile ) {
         #-- Try updating the Audit Tables again
         $AuditTableErrorLatch = $False;
         unlink $RetryFile;
         PrintInfoToBatchMessageLog("*** RETRY.flg detected.  Re-enabling Audit Table updates.");
         PrintInfoToBatchMessageLog("RETRY.flg removed.");
      }

      if ( $CurrLoopingPoint >= ($LastLoopingPoint + $JobPollInterval) ) {
         #-- The JobPollInterval time has elapsed
         $LastLoopingPoint = ElapsedSecondsFromSeedDate();

         #-- Determine how many jobs are currently running
         $RunningProcessesCounter = 0;
         for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
            $ProcessName = $ProcessList[$x][0];
            if ( $ProcessStatusList{$ProcessName}[5] eq "RUNNING" ) {
               $RunningProcessesCounter++;
            }
         }

         #-- ####################################################################
         #-- Launch jobs that are free to be launched
         if ( !$PauseMode && !$StopMode ) {
            #-- Not in Pause mode and not in Stop mode
            for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
               $ProcessName = $ProcessList[$x][0];

               if ( ProcessIsReadyToLaunch($ProcessName) && 
                     ($MaxParallelJobs == 0 || $RunningProcessesCounter < $MaxParallelJobs)) {
                  #-- Job is wating to run and all dependencies are met
                  $ProcessCommand = $ProcessFileNameList{$ProcessName}[0];
                  $RunningProcessesCounter++;

                  if ( $opt_x ) {
                     #-- TestMode is active
                     PrintInfoToBatchMessageLog(" *** TEST MODE ==> Simulating $ProcessName...");
                     $ProcessStatusList{$ProcessName}[2] = $RunOrder;
                     $ProcessStatusList{$ProcessName}[3] = "NULL";
                     $ProcessStatusList{$ProcessName}[4] = "NULL";
                     $ProcessStatusList{$ProcessName}[5] = "RUNNING";
                     $ProcessStatusList{$ProcessName}[6] = NowDate();
                     $RunOrder++;
                     $TestJobCounterList{$ProcessName} = 0;
                  }
                  else {
                     #-- Milestone job or Real job
                     if ( $ProcessName =~ /Milestone/ ) {
                        #-- Milestone job - Fake run it
                        PrintInfoToBatchMessageLog("$ProcessName reached.");
                        $ProcessStatusList{$ProcessName}[2] = $RunOrder;
                        $ProcessStatusList{$ProcessName}[3] = "NULL";
                        $ProcessStatusList{$ProcessName}[4] = "NULL";
                        $ProcessStatusList{$ProcessName}[5] = "RUNNING";
                        $ProcessStatusList{$ProcessName}[6] = NowDate();
                        $RunOrder++;
                        $MilestoneJobCounterList{$ProcessName} = 0;
                     }
                     else {
                        #-- Real job
                        #-- Create new job object
                        $oProcess = Proc::Simple->new();

                        #-- Run it
                        if ( $oProcess->start("$ProcessCommand") ) {
                           $ProcessStatusList{$ProcessName}[2] = $RunOrder;
                           $ProcessStatusList{$ProcessName}[3] = $oProcess;
                           $ProcessStatusList{$ProcessName}[4] = $oProcess->pid;
                           $ProcessStatusList{$ProcessName}[5] = "RUNNING";
                           $ProcessStatusList{$ProcessName}[6] = NowDate();
                           $RunOrder++;
                           PrintInfoToBatchMessageLog("Starting Job: $ProcessName (Pid: $ProcessStatusList{$ProcessName}[4])...");
                        }
                        else {
                           #-- Can't start job, something has gone terribly wrong
                           $JobControlFailed = $True;
                           PrintInfoToBatchMessageLog("Error: Can't launch $ProcessName.");
                        }
                     }
                  }
               }
            }
         }

         #-- ####################################################################
         #-- Determine status of each job
         #-- This is a global variable to maintain consistency of logs
         $NowEndTime = NowDate();

         for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
            $ProcessName = $ProcessList[$x][0];
            if ( $ProcessStatusList{$ProcessName}[5] eq "RUNNING" ) {
               #-- Job is running
               if ( $opt_x ) {
                  #-- TestMode
                  $TestJobCounterList{$ProcessName}++;
                  if ( $TestJobCounterList{$ProcessName} gt 1 ) {
                     #-- This is the 2nd time through the loop for this test job
                     #-- Test jobs always complete successfuly
                     $ProcessStatusList{$ProcessName}[5] = "SUCCESSFUL";
                     $ProcessStatusList{$ProcessName}[7] = $NowEndTime;
                  }
               }
               else {
                  #-- Milestone job or Real job
                  if ( $ProcessName =~ /Milestone/ ) {
                     #-- Milestone job
                     $MilestoneJobCounterList{$ProcessName}++;
                     if ( $MilestoneJobCounterList{$ProcessName} gt 1 ) {
                        #-- This is the 2nd time through the loop for this Milestone job
                        #-- Milestones always complete successfuly
                        $ProcessStatusList{$ProcessName}[5] = "SUCCESSFUL";
                        $ProcessStatusList{$ProcessName}[7] = $NowEndTime;
                     }
                  }
                  else {
                     #-- Real job
                     $oProcess = $ProcessStatusList{$ProcessName}[3];
                     #-- poll() returns 1 when running, 0 when not
                     $ProcessPollStatus = $oProcess->poll();
                     if ( $ProcessPollStatus == 0 ) { 
                        #-- Job has finished
                        $ProcessExitStatus = $oProcess->exit_status();

                        #-- Handle -1 Proc::Simple exit_status() return value issue
                        if ( $ProcessExitStatus == -1 ) {
                           #-- This happens very rarely, but is always a false failure
                           #-- Look at the Process Message Log file to see what the return code really is
                           #-- This work around assumes that the last line of the log is an echo of the status
                           PrintInfoToBatchMessageLog("*** WARNING: Unusual Proc::Simple exit_status() value: $ProcessExitStatus");
                           PrintInfoToBatchMessageLog("*** Verifying true exit_status by mining ProcessMessageLog");
                           if ( open(fhProcessMessageLog, "<$ProcessFileNameList{$ProcessName}[1]") ) {
                              #-- Goto bottom of file and mine return code there
                              if ( seek(fhProcessMessageLog, -20, 2) ) {
                                 my ($ChoppedLine, $KshRetVal) = split(/\:/, <fhProcessMessageLog>, 2);
                                 if ( $KshRetVal == 0 ) {
                                    PrintInfoToBatchMessageLog("*** ProcessMessageLog indicates SUCCESS (0) - Overrriding exit_status()");
                                    $ProcessExitStatus = 0;
                                 }
                              }
                              else {
                                 PrintInfoToBatchMessageLog("*** ERROR: Unable to seek end of ProcessMessageLog for exit_status verification.");
                              }
                              close fhProcessMessageLog;
                           }
                           else {
                              PrintInfoToBatchMessageLog("*** ERROR: Unable to open ProcessMessageLog for exit_status verification.");
                           }
                        }

                        if ( $ProcessExitStatus == 0 ) {
                           #-- Job completed successfully
                           PrintInfoToBatchMessageLog("   $ProcessName completed successfully ($ProcessExitStatus).");
                           $ProcessStatusList{$ProcessName}[5] = "SUCCESSFUL";
                           $ProcessStatusList{$ProcessName}[7] = $NowEndTime;
                        }
                        else {
                           #-- Job Failed
                           PrintInfoToBatchMessageLog("   $ProcessName FAILED ($ProcessExitStatus).");
                           $ProcessStatusList{$ProcessName}[5] = "FAILED";
                           $ProcessStatusList{$ProcessName}[7] = $NowEndTime;
                           SendJobStatusMessage($ProcessName);
                        }
                     }
                     else {
                        if ( $ProcessPollStatus != 1 ) { 
                           PrintInfoToBatchMessageLog("*** WARNING: Unknown Proc::Simple Poll() value: $ProcessPollStatus");
                        }
                     }
                  }
               }
            }
         }

         #-- ####################################################################
         #-- Set the overall status flags
         $OverallProcessRunning = $False;
         $OverallProcessWaiting = $False;
         for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
            $ProcessName = $ProcessList[$x][0];
            if ( $ProcessStatusList{$ProcessName}[5] eq "RUNNING" ) {
               $OverallProcessRunning = $True;
            }
            else {
               if ( $ProcessStatusList{$ProcessName}[5] eq "FAILED" ) {
                  $OverallProcessFailed = $True;
               }
               else {
                  if ( $ProcessStatusList{$ProcessName}[5] eq "WAITING" ) {
                     $OverallProcessWaiting = $True;
                  }
               }
            }
         }

         #-- Set the overall status
         if ( $OverallProcessRunning ) {
            $BatchAuditStatus = "RUNNING";
         }
         else {
            $BatchAuditStatus = "WAITING";
         }

         #-- ####################################################################
         #-- Write out audit logs
         if ( ! WriteAuditLogs($BatchAuditStatus) ) {
            $JobControlFailed = $True;
         }

         #-- ####################################################################
         #-- Update audit tables
         #-- This can happen only less than or as frequently than JobPollInterval
         if ( $PerformAuditTableUpdates eq "Y" ) {
            if ( $CurrLoopingPoint >= ($LastAuditPoint + $AuditTableUpdateInterval) ) {
               #-- The AuditTableUpdateInterval time has elapsed
               $LastAuditPoint = ElapsedSecondsFromSeedDate();
               if ( ! UpdateAuditTables($BatchAuditStatus) ) {
                  $JobControlFailed = $True;
               }
            }
         }

         if ( $DebugMode ) {
            PrintInfo("*Debug* -----------------------------------------------");
            PrintInfo("*Debug* LoopCounter: $LoopCounter");
            PrintInfo("*Debug* CurrLoopingPoint: $CurrLoopingPoint");
            for ($x = 0; $ProcessList[$x][0]; $x++) {
               $ProcessName = $ProcessList[$x][0];
               PrintInfo("*Debug* $ProcessName\t $ProcessStatusList{$ProcessName}[5]");
            }
         }

         #-- ####################################################################
         #-- Determine if we need to continue looping
         if ( ! $JobControlFailed ) {
            #-- No critical job control errors at this point
            if ( ! $OverallProcessFailed ) {
               #-- No jobs have failed at this point
               if ( $OverallProcessRunning || $OverallProcessWaiting ) {
                  #-- Jobs are either running or waiting
                  if ( $OverallProcessRunning || ! $OverallProcessWaiting ) {
                     #-- Jobs are running
                     $KeepLooping = $True;
                  }
                  else {
                     #-- Jobs are waiting and no jobs are running
                     if ( !$StopMode ) {
                        #-- Not in Stop mode
                        #-- At least one job should start next loop
                        $KeepLooping = $True;
                     }
                     else {
                        #-- Stop mode.  Shut down with error
                        $KeepLooping = $False;
                     }
                  }
               }
               else {
                  #-- All jobs must have completed
                  $KeepLooping = $False;
               }
            }
            else {
               #-- At least one job has failed
               if ( $OverallProcessRunning ) {
                  #-- Keep going only as long as jobs are running
                  $KeepLooping = $True;
               }
               else {
                  $KeepLooping = $False;
               }
            }
         }
         else {
            #-- Critical error in job Control - PANIC
            $KeepLooping = $False;
         }

         if ( $DebugMode ) {
            PrintInfo("*Debug* OverallProcessRunning: $OverallProcessRunning");
            PrintInfo("*Debug* OverallProcessWaiting: $OverallProcessWaiting");
            PrintInfo("*Debug* OverallProcessFailed : $OverallProcessFailed");
            PrintInfo("*Debug* KeepLooping          : $KeepLooping");
         }
      }
      #-- ####################################################################
      #-- Loop speed control
      sleep 1;
   }
   #-- ======================== Bottom of Loop ===============================

   #-- ####################################################################
   #-- Set return code
   $BatchAuditStatus = "SUCCESSFUL";
   if ( ! $JobControlFailed ) {
      #-- No critical job control error(s)
      if ( $OverallProcessFailed ) {
         #-- At least one job failed or we stopped on purpose
         $BatchAuditStatus = "FAILED";
         $RetVal = 6;
      }
      else {
         if ( $StopMode ) {
            #-- Stopping on purpose
            $BatchAuditStatus = "FAILED";
            $RetVal = 5;
         }
      }
   }
   else {
      #-- Critical job control error(s)
      $BatchAuditStatus = "FAILED";
      $RetVal = 2;
   }
   return $RetVal;
}

##############################################################################
sub IsFile
{
   my ($InFile) = @_;
   my $RetVal = $False;

   if ( -f $InFile && !(-l $InFile) ) {
      $RetVal = $True;
   }
   return $RetVal;
}

##############################################################################
sub IsDir
{
   my ($InDir) = @_;
   my $RetVal = $False;

   if ( -d $InDir ) {
      $RetVal = $True;
   }
   return $RetVal;
}

##############################################################################
sub ArchiveWorkLogs
{
   my $RetVal = $True;
   my @Files;
   my @Dirs;
   my $File;
   my $Dir;
   my $ArchiveLogFileDirectory;
   my $x;

   PrintInfoToBatchMessageLog("Archiving work logs...");
   #-- Gather all files in the log area
   opendir(dhLogFileDirectory, $LogFileDirectory);
   @Files = readdir(dhLogFileDirectory);
   closedir(dhLogFileDirectory);

   #-- Create log batch log directory
   $ArchiveLogFileDirectory = "$LogFileDirectory/archive/$BatchNumber.$RunNumber";
   PrintInfoToBatchMessageLog("Archiving to: $ArchiveLogFileDirectory");
   mkpath("$ArchiveLogFileDirectory");

   #-- Copy all work files
   foreach $File (@Files) {
      if ( IsFile("$LogFileDirectory/$File") ) {
         copy("$LogFileDirectory/$File",
              "$ArchiveLogFileDirectory/$File");
      }
   }

   if ( $MaxArchivedLogs > 0 ) {
      PrintInfoToBatchMessageLog("Purging archived work logs...");
      #-- Delete BatchWorkLogDirectories according to MaxArchivedLogs
      $ArchiveLogFileDirectory = "$LogFileDirectory/archive";
      opendir(dhArchiveLogFileDirectory, $ArchiveLogFileDirectory);
      @Dirs = readdir(dhArchiveLogFileDirectory);
      closedir(dhArchiveLogFileDirectory);

      #-- Sort list
      @Dirs = sort {$b cmp $a} (@Dirs);

      $x = 0;
      foreach $Dir (@Dirs) {
         if ( IsDir("$LogFileDirectory/archive/$Dir") ) {
            if ( ! ("$Dir" eq '.' || "$Dir" eq '..' ) ) {
               $x++;
               if ( $x > $MaxArchivedLogs ) {
                  $ArchiveLogFileDirectory = "$LogFileDirectory/archive/$Dir";
                  PrintInfoToBatchMessageLog("Purging: $ArchiveLogFileDirectory");
                  rmtree("$ArchiveLogFileDirectory");
               }
            }
         }
      }
   }
   else {
      PrintInfoToBatchMessageLog("Not purging archived logs.");
   }
   return $RetVal;
}

##############################################################################
sub BuildFileNameList
{

   my $RetVal = $True;
   my $x = 0;
   my $ProcessName = "";
   my $CleansedProcessName = "";
   my $ProcessMessageLog = "";
   my $ProcessFile = "";
   my $ProcessCommand = "";

   for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
      $ProcessName         = $ProcessList[$x][0];
      $CleansedProcessName = $ProcessName;
      $CleansedProcessName =~ tr/\./_/; 
      $ProcessMessageLog   = "$LogFileDirectory"."$Slash"."$BatchName"."_$CleansedProcessName"."_ProcessMessage.log";
      $ProcessFile         = "$BinFileDirectory"."$Slash"."$ProcessName";
      $ProcessCommand      = "$BinFileDirectory"."$Slash"."$ProcessName $ConfigurationFile > $ProcessMessageLog 2>&1";

      #-- ProcessName => [ ProcessCommand, ProcessMessageLog ]
      $ProcessFileNameList{$ProcessName} = [ $ProcessCommand, $ProcessMessageLog ];

      if ( !($ProcessName =~ /Milestone/) ) {
         #-- Don't validate Milestone jobs
         if ( -r $ProcessFile ) {
            #-- job is readable by the current user
            if ( ! (-x $ProcessFile) ) {
               #-- Job is not executable by the current user
               $RetVal = $False;
               PrintInfoToBatchMessageLog("Error: Process is not executable: $ProcessFile");
            }
         }
         else {
            $RetVal = $False;
            PrintInfoToBatchMessageLog("Error: Process is not readable or does not exist: $ProcessFile");
         }
      }
   }
   return $RetVal;
}

##############################################################################
sub ConnectToTableMetadata
{
   my $RetVal = $False;

   if ( $PerformAuditTableUpdates eq "Y" ) {
      #-- Open connection to TableMetadata
      PrintInfoToBatchMessageLog("Connecting to table metadata...");
      $dbh = DBI->connect( "$BfConnectString",
                           $BfUserId, $BfUserPassword,
                           { AutoCommit => 0, RaiseError => 0, PrintError => 1 } );
      if ( $dbh ) {
         #-- Connected
         $RetVal = $True;
         PrintInfoToBatchMessageLog("Connected to table metadata.");
      }
      else {
         PrintInfoToBatchMessageLog("$DBI::errstr");
      }
   }
   else {
      $RetVal = $True;
   }
   return $RetVal;
}

##############################################################################
sub IteratePredecessors
{
   my ($RootProcess, $Process, $PredArrayRef, $ProcessChainHashRef, $ProcessChainArrayRef, $DeadlyEmbrace) = @_;

   my $Predecessor;
   my $x;

   #-- Add the passed job to the job chains
   $$ProcessChainHashRef{$Process}=1;
   push(@$ProcessChainArrayRef, "$Process");

   #-- Iterate the list of predecessor jobs for the passed in job
   for ( $x = 0; @$PredArrayRef[$x] && (!$$DeadlyEmbrace); $x++ ) {
      $Predecessor = @$PredArrayRef[$x];

      #-- Make sure this predecessor is not in the job chain yet
      if ( ! $$ProcessChainHashRef{$Predecessor} ) {
         #-- This job does not already exist in the chain of jobs that got me here

         #-- Check to see if this predecessor has its own predecessors
         if ( $ProcessPredecessorList{$Predecessor} ) {
            #-- This predecessor job has predecessors itself

            IteratePredecessors($RootProcess, $Predecessor, \@{$ProcessPredecessorList{$Predecessor}},
                                $ProcessChainHashRef, $ProcessChainArrayRef, $DeadlyEmbrace);
         }
      }
      else {
         $$DeadlyEmbrace = 1;
         PrintInfoToBatchMessageLog("Error: Deadly Embrace detected.");
         PrintInfoToBatchMessageLog("Circular job reference chain:");
         for ($x = 0; $$ProcessChainArrayRef[$x]; $x++) {
            PrintInfoToBatchMessageLog("   $$ProcessChainArrayRef[$x]");
         }
         PrintInfoToBatchMessageLog("   $Predecessor");
      }
   }
   delete($$ProcessChainHashRef{$Process});
   pop(@$ProcessChainArrayRef);
   return
}

##############################################################################
sub BuildPredecessorList
{
   my $RetVal = $True;
   my $ProcessName = "";
   my @Predecessors = "";
   my $Predecessor = "";
   my $x = 0;
   my $y = 0;

   for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
      #-- Iterate through Process List array

      if ( $ProcessList[$x][1] ) {
         #-- This job has predecessors
         $ProcessName = $ProcessList[$x][0];
         @Predecessors = split(" ", $ProcessList[$x][1]);
         
         #-- ProcessName => [ Predecessors ]
         $ProcessPredecessorList{$ProcessName} = [ @Predecessors ];

         #-- Validate all  predecessor jobs
         for ( $y = 0; $Predecessors[$y]; $y++ ) {
            $Predecessor = $Predecessors[$y];

            #-- Make sure predecessor job is also listed as a run job
            if ( !($ProcessStatusList{$Predecessor}) ) {
               #-- This predecessor jobs is not listed as a run job
               $RetVal = $False;
               PrintInfoToBatchMessageLog("Error: Predecessor process, $Predecessor, is not listed as a job in the ProcessListFile");
            }
         }
      }
   }
   
   #-- Make sure there are no Deadly Embraces
   if ( $RetVal ) {
      #-- OK so far...
      PrintInfoToBatchMessageLog("Checking for Deadly Embraces...");
      my %ProcessChainHash = ();
      my @ProcessChainArray = ();
      my $DeadlyEmbrace = $False;

      #-- Spin through each Process and its Predecessor list
      foreach $ProcessName (sort(keys %ProcessPredecessorList)) {
         #-- This is a recursive function that will follow each Predecessor looking for circular references
         IteratePredecessors($ProcessName, $ProcessName, \@{$ProcessPredecessorList{$ProcessName}},
                             \%ProcessChainHash, \@ProcessChainArray, \$DeadlyEmbrace);
         if ( $DeadlyEmbrace ) {
            $RetVal = $False;
            last;
         }
      }

   }
   return $RetVal;
}
                  
##############################################################################
sub BuildStatusList
{
   my $RetVal = $True;
   my $x = 0;
   my $ResurrectLogFile = "";
   my @ResurrectLogArray = ();
   my $SomethingToResurrect = $False;
   my $ThisBatchNumber = "";
   my $ThisRunNumber = "";
   my $ThisProcessName = "";
   my $ThisStatus = "";
   my $ThisStartTime = "";
   my $ThisEndTime = "";
   my $MaxRunNumber = 0;

   #-- Initialize the status list as if we never ran it yet
   for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
      #-- ProcessName => [ RunNumber, NaturalOrder, RunOrder, oProcess, PID, Status, StartTime, EndTime ]
      $ProcessStatusList{$ProcessList[$x][0]} = [ 1, $x, 0, 0, 0, "WAITING", "", "" ];
   }

   if ( $DebugMode ) {
      PrintInfo("*Debug* ProcessStatusList after initialization:");
      PrintInfo("*Debug* ProcessName|RunNumber|NaturalOrder|RunOrder|oProcess|PID|Status|StartTime|EndTime");
      for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
         $ThisProcessName = $ProcessList[$x][0];
         PrintInfo("*Debug* ProcessStatusList --> $ThisProcessName".
                   "|$ProcessStatusList{$ThisProcessName}[0]".
                   "|$ProcessStatusList{$ThisProcessName}[1]".
                   "|$ProcessStatusList{$ThisProcessName}[2]".
                   "|$ProcessStatusList{$ThisProcessName}[3]".
                   "|$ProcessStatusList{$ThisProcessName}[4]".
                   "|$ProcessStatusList{$ThisProcessName}[5]".
                   "|$ProcessStatusList{$ThisProcessName}[6]".
                   "|$ProcessStatusList{$ThisProcessName}[7]");
      }
   }

   #-- Attempt to resurrect
   if ( $ResurrectMode ) {
      PrintInfoToBatchMessageLog("Attempting to resurrect the last run of this batch...");
      $ResurrectLogFile = $ProcessAuditLogFile;
      #-- Open the ResurrectLogFile
      PrintInfoToBatchMessageLog("Attempting to resurrect from: $ResurrectLogFile");

      if ( -r $ResurrectLogFile ) {
         #-- Last process audit log file exists

         if ( open(fhResurrectLogFile, "<$ResurrectLogFile") ) {
            $x = 0;
            while ( <fhResurrectLogFile> ) {
               chop;
               $x++;
               #-- Parse incoming line
               (
                  $ThisBatchNumber,
                  $ThisRunNumber,
                  $ThisProcessName,
                  $ThisStatus,
                  $ThisStartTime,
                  $ThisEndTime
               ) = split (/\|/,$_,6);

               #-- Update max RunNumber
               if ( $ThisRunNumber > $MaxRunNumber ) {
                  $MaxRunNumber = $ThisRunNumber
               }
               if ( $ThisStatus eq "RUNNING" || $ThisStatus eq "WAITING" || $ThisStatus eq "FAILED" ) {
                  $SomethingToResurrect = $True;
               }
               push(@ResurrectLogArray, [$ThisBatchNumber, $ThisRunNumber, $ThisProcessName, $ThisStatus, $ThisStartTime, $ThisEndTime]);
            }
            close fhResurrectLogFile;

            if ( $SomethingToResurrect ) {
               PrintInfoToBatchMessageLog("Resurrecting last batch...");
               for ( $x = 0; $ResurrectLogArray[$x][0]; $x++ ) {

                  $ThisBatchNumber = $ResurrectLogArray[$x][0];
                  $ThisRunNumber =   $ResurrectLogArray[$x][1];
                  $ThisProcessName = $ResurrectLogArray[$x][2];
                  $ThisStatus =      $ResurrectLogArray[$x][3];
                  $ThisStartTime =   $ResurrectLogArray[$x][4];
                  $ThisEndTime =     $ResurrectLogArray[$x][5];

                  #-- Update the status list based on the last batch status
                  #-- Possible statuses:
                  #--    SUCCESSFUL - Treat these as done.
                  #--    RUNNING    - Treat these as to be resurrected.
                  #--    WAITING    - Treat these as to be resurrected.
                  #--    FAILED     - Treat these as to be resurrected.
                  #-- ProcessName => [ RunNumber, NaturalOrder, RunOrder, oProcess, PID, Status, StartTime, EndTime ]
                  if ( $ThisStatus eq "SUCCESSFUL" ) {
                     $ProcessStatusList{$ThisProcessName}[0] = $ThisRunNumber;
                     $ProcessStatusList{$ThisProcessName}[2] = $x+1;
                     $ProcessStatusList{$ThisProcessName}[5] = $ThisStatus;
                     $ProcessStatusList{$ThisProcessName}[6] = $ThisStartTime;
                     $ProcessStatusList{$ThisProcessName}[7] = $ThisEndTime;
                  }
                  else {
                     $ProcessStatusList{$ThisProcessName}[0] = $ThisRunNumber + 1;
                     $ProcessStatusList{$ThisProcessName}[5] = "WAITING";
                     $ProcessStatusList{$ThisProcessName}[6] = "";
                     $ProcessStatusList{$ThisProcessName}[7] = "";
                  }
               } 
               #-- Set BatchNumber and RunNumber for resurrecting
               $BatchNumber = $ThisBatchNumber;
               $RunNumber = $MaxRunNumber + 1;
            }
            else {
               PrintInfoToBatchMessageLog("Nothing to resurrect in Process Log file.");
            }

            if ( $DebugMode ) {
               PrintInfo("*Debug* ProcessStatusList after initialization when resurrecting:");
               PrintInfo("*Debug* ProcessName|RunNumber|NaturalOrder|RunOrder|oProcess|PID|Status|StartTime|EndTime");
               for ( $x = 0; $ProcessList[$x][0]; $x++ ) {
                  $ThisProcessName = $ProcessList[$x][0];
                  PrintInfo("*Debug* ProcessStatusList --> $ThisProcessName".
                            "|$ProcessStatusList{$ThisProcessName}[0]".
                            "|$ProcessStatusList{$ThisProcessName}[1]".
                            "|$ProcessStatusList{$ThisProcessName}[2]".
                            "|$ProcessStatusList{$ThisProcessName}[3]".
                            "|$ProcessStatusList{$ThisProcessName}[4]".
                            "|$ProcessStatusList{$ThisProcessName}[5]".
                            "|$ProcessStatusList{$ThisProcessName}[6]".
                            "|$ProcessStatusList{$ThisProcessName}[7]");
               }
            }
         }
         else {
            #-- Can't open resurrect file
            PrintInfoToBatchMessageLog("Error: Unable to open ResurrectLogFile: $ResurrectLogFile");
            $RetVal = $False;
         }
      }
      else {
         PrintInfoToBatchMessageLog("Process Log not found.  Nothing to resurrect.");
      }
   }
   else {
      PrintInfoToBatchMessageLog("NOT attempting to resurrect the last batch.");
   }
   return $RetVal;
}
                  
##############################################################################
sub RemoveCommentedOutProcesses
{
   my ($Predecessors_List) = (@_);
   my @Predecessors_Array = ();
   my $Predecessor;
   my $CommentedOutProcess;
   my $CommentedOut = $False;
   my $x;
   my $RetVal = "";

   #-- Convert list to an array
   @Predecessors_Array = split(" ", $Predecessors_List);

   #-- Spin through Predecessors and return only the ones that are not commented out
   foreach $Predecessor (@Predecessors_Array) {
      #-- Look for the Predecessor in the comment out list
      #-- If it exists in the comment out list
      #--    Look for the Predecessor in the process list
      #--    If it exists in the process list, leave it in the predecessor list
      #--    If it does not exist in the process list, remove it from the predecessor list
      #-- If it does not exist in the comment out list, leave it in the predecessor list

      $Predecessor = AllTrim($Predecessor);
      $CommentedOut = $False;
      #-- See if this predecessor is commented out
      foreach $CommentedOutProcess (@CommentedOutProcessList) {
         if ( "$Predecessor" eq "$CommentedOutProcess" ) {
            #-- It is commented out
            $CommentedOut = $True;
            #-- See if this predecessor is in process list
            for ($x = 0; $ProcessList[$x][0]; $x++) {
               if ( "$Predecessor" eq "$ProcessList[$x][0]" ) {
                  #-- It is in the process list, keep it
                  $CommentedOut = $False;
               }
            }
            last;
         }
      }
      if ( ! $CommentedOut ) {
         #-- Keep this one
         $RetVal = $RetVal." ".$Predecessor;
      }
   }
   return AllTrim($RetVal);
}

##############################################################################
sub BuildProcessList
{
   my $RetVal = $False;
   my $RowCount = 0;
   my $ProcessCount = 0;
   my $CommentedOutProcessCount = 0;
   my $x = 0;
   my $y = 0;
   my $DuplicateProcess = $False;

   my $ProcessName = "";
   my $Predecessors = "";
   my $BatchName = "";

   if ( open(fhProcessListFile, "<$ProcessListFile") ) {

      #-- Build @CommentedOutProcessList
      while ( <fhProcessListFile> ) {
         $RowCount++;
         if ( $RowCount > 1 ) {
            chop;
            if ( AllTrim($_) ) {
               #-- Not a blank line
               if ( ! ($_ =~ /^ *\#\-\-/) ) {
                  #-- Not a descriptive comment
                  #-- Parse incoming line
                  ($ProcessName) = split (/\,/,$_,10);
                  $ProcessName = AllTrim($ProcessName);
                  if ( substr($ProcessName, 0, 1) eq "#" ) {
                     #-- This item is commented out
                     #-- Strip off the comment character
                     $ProcessName = AllTrim(substr($ProcessName,index($ProcessName,'#')+1));
                     push(@CommentedOutProcessList, $ProcessName);
                     $CommentedOutProcessCount++;
                  }
               }
            }
         }
      }

      #-- Reposition back to top of file
      seek(fhProcessListFile, 0, 0);
      $RowCount = 0;

      #-- Build @ProcessList
      while ( <fhProcessListFile> ) {
         $RowCount++;
         if ( $RowCount > 1 ) {
            chop;
            if ( AllTrim($_) ) {
               #-- Not a blank line
               if ( ! ($_ =~ /^ *\#/) ) {
                  #-- Not a comment of any kind
                  #-- Parse incoming line
                  (
                     $ProcessName,
                     $Predecessors
                  ) = split (/\,/,$_,10);

                  #-- prep each field for array handling
                  $ProcessName                   = AllTrim($ProcessName);
                  $Predecessors                  = SquashSpaces(AllTrim($Predecessors));

                  push(@ProcessList, [$ProcessName, $Predecessors]);
                  $ProcessCount++;
               }
            }
         }
      }
      close fhProcessListFile;
      if ( $ProcessCount > 0 ) {
         #-- At least one job to run
         #-- Make sure no processes are duplicated
         for ($x = 0; $x < $ProcessCount-1 && ! $DuplicateProcess; $x++) {
            for ($y = $x+1; $y < $ProcessCount; $y++) {
               if ( "$ProcessList[$x][0]" eq "$ProcessList[$y][0]" ) {
                  PrintInfoToBatchMessageLog("Error: Duplicate process in ProcessList: $ProcessList[$x][0]");
                  $DuplicateProcess = $True;
                  last;
               }
            }
         }
         if ( ! $DuplicateProcess ) {
            #-- No duplicate processes

            #-- Thin out Predecessors of commented out processes
            for ($x = 0; $ProcessList[$x][0]; $x++) {
               $ProcessList[$x][1] = RemoveCommentedOutProcesses($ProcessList[$x][1]);
            }

            #-- Send list to log file
            PrintInfoToBatchMessageLog("Processes:");
            for ($x = 1; $ProcessList[$x-1][0]; $x++) {
               PrintInfoToBatchMessageLog("   $x) $ProcessList[$x-1][0]");
            }
            $RetVal = $True;
         }
      }
      else {
         PrintInfoToBatchMessageLog("Error: Empty ProcessListFile: $ProcessListFile");
      }
   }
   else {
      PrintInfoToBatchMessageLog("Error: Unable to open ProcessListFile: $ProcessListFile");
   }
   return $RetVal;
}

##############################################################################
sub LookupLastSuccessViaLogFile
{
   my $RetVal = $False;
   my $ThisBatchNumber = "";
   my $ThisRunNumber = "";
   my $ThisBatchName = "";
   my $ThisProcessDate = "";
   my $ThisStatus = "";
   my $ThisBatchType = "";
   my $ThisBatchAlias = "";
   my $ThisStartTime = "";
   my $ThisEndTime = "";

   my $LSBatchNumber = "NONE";
   my $LSRunNumber   = "NONE";
   my $LSProcessDate = "NONE";

   #-- Lookup last successful stuff
   if ( -r $BatchHistoryLogFile ) {
      #-- There is a history log file
      if ( open(fhBatchHistoryLogFile, "<$BatchHistoryLogFile") ) {
         $RetVal = $True;
         while ( <fhBatchHistoryLogFile> ) {
            chop;

            #-- Parse history row
            (
               $ThisBatchNumber,
               $ThisRunNumber,
               $ThisBatchName,
               $ThisProcessDate,
               $ThisStatus,
               $ThisStartTime,
               $ThisEndTime,
               $ThisBatchType,
               $ThisBatchAlias
            ) = split (/\|/,$_,9);

            if ( "$ThisBatchName" eq "$BatchName" ) {
               if ( $ThisStatus eq "SUCCESSFUL" ) {
                  $LSBatchNumber = $ThisBatchNumber;
                  $LSRunNumber   = $ThisRunNumber;
                  $LSProcessDate = $ThisProcessDate;
               }
            }
         }
         if ( "$LSBatchNumber" ne "NONE" ) {
            #-- Found at least one successful run
            $LastSuccessfulBatchNumber = $LSBatchNumber;
            $LastSuccessfulRunNumber   = $LSRunNumber;
            $LastSuccessfulProcessDate = $LSProcessDate;
         }
         else {
            #-- This subject area has never run successfully
            $LastSuccessfulBatchNumber = 19000101000001;
            $LastSuccessfulRunNumber   = 0;
            $LastSuccessfulProcessDate = "1900-01-01 00:00:01";
         }
      }
   }
   else {
      #-- No history file
      $RetVal = $True;
      $LastSuccessfulBatchNumber = 19000101000001;
      $LastSuccessfulRunNumber   = 0;
      $LastSuccessfulProcessDate = "1900-01-01 00:00:01";
   }
   return $RetVal;
}

##############################################################################
sub LookupLastSuccessViaTableMetadata
{
   my $RetVal = $False;
   my $ThisBatchNumber = "";
   my $ThisRunNumber = "";
   my $ThisProcessDate = "";
   my $SQL;
   my $sth;

   $SQL = "SELECT batch_number, run_number, DATE_FORMAT(process_date, '%Y-%m-%d %T')
             FROM etl_batch_audit,
                  (select max(batch_number) as max_batch_number
                     FROM etl_batch_audit
                    WHERE system_name = ?
                      AND batch_name = ?
                      AND batch_status = 'SUCCESSFUL') max_batch_audit
            WHERE batch_number = max_batch_audit.max_batch_number
              AND system_name = ?
              AND batch_name = ?
              AND batch_status = 'SUCCESSFUL'";

   $sth = $dbh->prepare( $SQL );
   $sth->execute($ApplicationName, $BatchName, $ApplicationName, $BatchName);
   $sth->bind_columns( \( $ThisBatchNumber, $ThisRunNumber, $ThisProcessDate ) );
   if ( $sth->fetch ) {
      if ( $sth->err ) {
         PrintInfoToBatchMessageLog("Error: $DBI::errstr");
      }
      $RetVal = $True;
      $LastSuccessfulBatchNumber = $ThisBatchNumber;
      $LastSuccessfulRunNumber   = $ThisRunNumber;
      $LastSuccessfulProcessDate = $ThisProcessDate;
   }
   else {
      $RetVal = $True;
      $LastSuccessfulBatchNumber = 19000101000001;
      $LastSuccessfulRunNumber   = 0;
      $LastSuccessfulProcessDate = "1900-01-01 00:00:01";
   }
   return $RetVal;
}

##############################################################################
sub LookupLastSuccess
{
   my $RetVal = $False;

   if ( $PerformAuditTableUpdates eq "Y" ) {
      #-- Get last success data from table metadata
      PrintInfoToBatchMessageLog("Getting last run statistics from table metadata...");
      if ( LookupLastSuccessViaTableMetadata() ) {
         PrintInfoToBatchMessageLog("Last run statistics from table metadata retrieved.");
         $RetVal = $True;
      }
   }
   else {
      PrintInfoToBatchMessageLog("Getting last run statistics from log file metadata...");
      if ( LookupLastSuccessViaLogFile() ) {
         PrintInfoToBatchMessageLog("Last run statistics from log file metadata retrieved.");
         $RetVal = $True;
      }
   }
   return $RetVal;
}

##############################################################################
sub LastRunSucceeded
{
   my $x;
   my $ThisBatchNumber;
   my $ThisRunNumber;
   my $ThisProcessName;
   my $ThisStatus;
   my $ThisStartTime;
   my $ThisEndTime;
   my $RetVal = $True;

   if ( -r $ProcessAuditLogFile ) {
      #-- Last process audit log file exists
      if ( open(fhProcessAuditLogFile, "<$ProcessAuditLogFile") ) {
         $x = 0;
         while ( <fhProcessAuditLogFile> ) {
            chop;
            $x++;
            #-- Parse incoming line
            (
               $ThisBatchNumber,
               $ThisRunNumber,
               $ThisProcessName,
               $ThisStatus,
               $ThisStartTime,
               $ThisEndTime
            ) = split (/\|/,$_,6);
            if ( $ThisStatus eq "RUNNING" || $ThisStatus eq "WAITING" || $ThisStatus eq "FAILED" ) {
               $RetVal = $False;
               last;
            }
         }
         close fhProcessAuditLogFile;
      }
      else {
         #-- Can't open ProcessAuditLogFile
         die PrintInfo("Error: Unable to open ProcessAuditLogFile: $ProcessAuditLogFile");
      }
   }
   return $RetVal;
}

##############################################################################
sub ClearWorkArea
{
   my $x = 0;
   my $RetVal = $False;

   #-- Delete all Batch log files
   opendir (DIR, $LogFileDirectory) or die $!;
   while ( my $DelFile = readdir(DIR)) {
      if ( -f "$LogFileDirectory/$DelFile" ) {
         unlink "$LogFileDirectory/$DelFile";

      }
   }
   closedir(DIR);

   #-- Delete all Batch work files
   opendir (DIR, $WorkFileDirectory) or die $!;
   while ( my $DelFile = readdir(DIR)) {
      if ( -f "$WorkFileDirectory/$DelFile" ) {
         unlink "$WorkFileDirectory/$DelFile";
      }
      else {
         if ( -d "$WorkFileDirectory/$DelFile" ) {
            if ( !("$DelFile" eq "." || "$DelFile" eq "..")) {
               rmtree("$WorkFileDirectory/$DelFile");
            }
         }
      }
   }  
   closedir(DIR);

   return $RetVal;
}

##############################################################################
sub GetParameterValue
{
   my ($VariableValuePair, $VariableName) = @_;
   my $StartOfValue = 0;
   my $Length = 0;
   my $RetVal = "";

   #-- Parse value after the equal sign
   $StartOfValue = index($VariableValuePair, "=") + 1;
   $Length = length($VariableValuePair);
   $RetVal = substr($VariableValuePair, $StartOfValue, $Length - $StartOfValue);

   #-- Strip of surrounding quotes if necessary
   $Length = length($RetVal);
   if ( substr($RetVal, 0, 1) eq '"' && substr($RetVal, $Length-1, 1) eq '"' ) {
      $RetVal = substr($RetVal, 1, $Length - 2);
   }

   return $RetVal;
}

##############################################################################
sub GetParameters
{
   my $RetVal = $False;

   if ( open(fhConfigurationFile, "<$ConfigurationFile") ) {
      while ( <fhConfigurationFile> ) {
         chop;
         $ApplicationName           = GetParameterValue($_, "ApplicationName")           if $_ =~ /^ApplicationName=/;
         $BatchName                 = GetParameterValue($_, "BatchName")                 if $_ =~ /^BatchName=/;
         $JobPollInterval           = GetParameterValue($_, "JobPollInterval")           if $_ =~ /^JobPollInterval=/;
         $MaxParallelJobs           = GetParameterValue($_, "MaxParallelJobs")           if $_ =~ /^MaxParallelJobs=/;
         $MaxArchivedLogs           = GetParameterValue($_, "MaxArchivedLogs")           if $_ =~ /^MaxArchivedLogs=/;
         $PerformAuditTableUpdates  = GetParameterValue($_, "PerformAuditTableUpdates")  if $_ =~ /^PerformAuditTableUpdates=/;
         $AuditTableUpdateInterval  = GetParameterValue($_, "AuditTableUpdateInterval")  if $_ =~ /^AuditTableUpdateInterval=/;
         $AuditTableCriticality     = GetParameterValue($_, "AuditTableCriticality")     if $_ =~ /^AuditTableCriticality=/;
         $BfConnectString           = GetParameterValue($_, "BfConnectString")           if $_ =~ /^BfConnectString=/;
         $BfUserId                  = GetParameterValue($_, "BfUserId")                  if $_ =~ /^BfUserId=/;
         $BfUserPassword            = GetParameterValue($_, "BfUserPassword")            if $_ =~ /^BfUserPassword=/;
         $BfBinFileDirectory        = GetParameterValue($_, "BfBinFileDirectory")        if $_ =~ /^BfBinFileDirectory=/;
         $BfLogFileDirectory        = GetParameterValue($_, "BfLogFileDirectory")        if $_ =~ /^BfLogFileDirectory=/;
         $BfLockFileDirectory       = GetParameterValue($_, "BfLockFileDirectory")       if $_ =~ /^BfLockFileDirectory=/;
         $BinFileDirectory          = GetParameterValue($_, "BinFileDirectory")          if $_ =~ /^BinFileDirectory=/;
         $LogFileDirectory          = GetParameterValue($_, "LogFileDirectory")          if $_ =~ /^LogFileDirectory=/;
         $PollFileDirectory         = GetParameterValue($_, "PollFileDirectory")         if $_ =~ /^PollFileDirectory=/;
         $WorkFileDirectory         = GetParameterValue($_, "WorkFileDirectory")         if $_ =~ /^WorkFileDirectory=/;
         $SendFailureMessage        = GetParameterValue($_, "SendFailureMessage")        if $_ =~ /^SendFailureMessage=/;
         $AlertEMailList            = GetParameterValue($_, "AlertEMailList")            if $_ =~ /^AlertEMailList=/;
      }
      close fhConfigurationFile;
      $RetVal = $True;
   }
   else  {
      PrintInfo("Error: Unable to open ConfigurationFile: $ConfigurationFile");
   }
   return $RetVal;
}

##############################################################################
sub CreateBatchNumberLockFile
{
   my ($BatchNumberLockFile) = @_;
   my $RetVal = $False;

   if ( -r $BatchNumberLockFile ) {
      #-- Lock file already exists
      $RetVal = $True;
   }
   else {
      #-- Create lock file
      if ( open(fhBatchNumberLockFile, ">$BatchNumberLockFile") ) {
         #-- Lock file created
         close fhBatchNumberLockFile;
         $RetVal = $True;
      }
   }
   return $RetVal;
}

##############################################################################
sub GetUniqueBatchNumber
{
   my ($BatchNumberOption) = @_;
   my $LockFile = $BfLockFileDirectory.$Slash.$ApplicationName."_BatchNumber.lok";
   my $LockFileIsLocked = $False;
   my $LockFileIsLockedTime = "";
   my $RetVal = $False;

   $BatchNumber = NowDate("YYYYMMDDHH24MISS");

   if ( $BatchNumberOption ) { 
      #-- BatchNumber option passed
      $BatchNumber = $BatchNumberOption;
      $RetVal = $True;
   }
   else {
      #-- Generate unique BatchNumber
      if ( CreateBatchNumberLockFile("$LockFile") ) {
         #-- Created lock file or it already exists
         while ( ! $LockFileIsLocked ) {
            if ( open(fhLockFile, "+<$LockFile") ) {
               #-- Lock the file
               if ( flock(fhLockFile, LOCK_EX | LOCK_NB) ) {
                  #-- Got the lock
                  $LockFileIsLocked = $True;
                  $LockFileIsLockedTime = NowDate("YYYYMMDDHH24MISS");

                  #-- Truncate the file
                  seek(fhLockFile, 0, 0);
                  truncate(fhLockFile, 0);

                  #-- Get the next number
                  $BatchNumber = $LockFileIsLockedTime;
                  while ( $BatchNumber le $LockFileIsLockedTime ) {
                     sleep 1;
                     $BatchNumber = NowDate("YYYYMMDDHH24MISS");
                  }
                  print fhLockFile "$BatchNumber\n";
               }
               #-- Close the lock file
               close fhLockFile;
            }
            if ( ! $LockFileIsLocked ) {
               sleep 1;
            }
         }
         $RetVal = $True;
      }
      else {
         #-- Can't create lock file
         PrintInfoToBatchMessageLog("Error: Can't create LockFile: $LockFile");
      }
   }
   return $RetVal;
}

##############################################################################
sub IsValidArgs
{
   my $RetVal = $False;

   #-- Be sure ConfigurationFile exists and is readable
   if ( -r $ConfigurationFile ) {
      #-- It's readable
      $RetVal = $True;
   }
   else {
      PrintInfo("Error: ConfigurationFile not readable: $ConfigurationFile");
   }
   return $RetVal;
}

##############################################################################
sub IsValidBatchTypeOption
{
   my $RetVal = $False;

   if ( $opt_x ) {
      #-- TestMode
      $BatchType = "TEST";
      $RetVal = $True;
   }
   else {
      #-- Not TestMode
      if ( $opt_t ) {
         #-- BatchType has been thrown
         if ( uc($opt_t) eq "AUTO" || uc($opt_t) eq "MANUAL") {
            $BatchType = $opt_t;
            $RetVal = $True;
         }
         else {
            PrintInfo("Error: Invalid BatchType: $opt_t");
         }
      }
      else {
         #-- BatchType option not set
         if ( defined($ENV{'RUN_BY_CRON'}) && $ENV{'RUN_BY_CRON'} eq "TRUE" ) {
            #-- we are running under cron
            $BatchType = "AUTO";
            $RetVal = $True;
         }
         else {
            #-- Not running under cron
            #-- use default
            $RetVal = $True;
         }
      }
   }
   return $RetVal;
}

##############################################################################
sub IsValidBatchAliasOption
{
   my $RetVal = $False;

   if ( $opt_a ) {
      #-- Alias provided
      #-- Make sure it does not contain spaces

      if ( $opt_a =~ / / ) {
         #-- Contains embedded spaces
         PrintInfo("Error: Invalid BatchAlias: $opt_a");
      }
      else {
         $RetVal = $True;
      }
   }
   else {
      #-- Alias Not provided
      $RetVal = $True;
   }
   return $RetVal;
}

##############################################################################
sub IsValidOptions
{
   my $RetVal = $False;

   if ( IsValidBatchTypeOption() ) {
      if ( IsValidBatchAliasOption() ) {
         $RetVal = $True;
      }
   }
   return $RetVal;
}

##############################################################################
sub IsValidAuditTableParameters
{
   my $RetVal = $False;

   if ( $PerformAuditTableUpdates eq "Y" ) {
      if ( $AuditTableUpdateInterval >= $JobPollInterval ) {
         if ( $ApplicationName ) {
            if ( $AuditTableCriticality eq "WARN" || $AuditTableCriticality eq "ERROR" ) {
               if ( $BfConnectString && $BfUserId && $BfUserPassword ) {
                  $RetVal = $True;
               }
               else {
                  PrintInfo("Error: BfConnectString, BfUserId, BfUserPassword cannot be blank in Parameter file.");
               }
            }
            else {
               PrintInfo("Error: AuditTableCriticality must either be WARN or ERROR in Parameter file.");
            }
         }
         else {
            PrintInfo("Error: ApplicationName cannot be blank in Parameter file.");
         }
      }
      else {
         PrintInfo("Error: AuditTableUpdateInterval must be >= JobPollInterval in Parameter file.");
      }
   }
   else {
      $RetVal = $True;
   }
   return $RetVal;
}

##############################################################################
sub IsValidParameters
{
   my $RetVal = $False;

   if ( -d $BfLogFileDirectory ) {
      if ( -d $BinFileDirectory ) {
         if ( -d $WorkFileDirectory ) {
            if ( -d $LogFileDirectory ) {
               if ( -d $PollFileDirectory ) {
                  if ( $ApplicationName ) {
                     if ( $BatchName ) {
                        #-- All required parameters are valid
                        if ( IsValidAuditTableParameters() ) {
                           $RetVal = $True;
                        }
                     }
                     else {
                        PrintInfo("Error: BatchName is missing.");
                     }
                  }
                  else {
                     PrintInfo("Error: ApplicationName is missing.");
                  }
               }
               else {
                  PrintInfo("Error: PollFileDirectory is missing.");
               }
            }
            else {
               PrintInfo("Error: LogFileDirectory is missing.");
            }
         }
         else {
            PrintInfo("Error: WorkFileDirectory is missing.");
         }
      }
      else {
         PrintInfo("Error: BinFileDirectory is missing.");
      }
   }
   else {
      PrintInfo("Error: BfLogFileDirectory is missing.");
   }
   return $RetVal;
}

##############################################################################
sub LockBatchExecLockFile
{
   my ($BatchExecLockFile) = @_;
   my $RetVal = $False;

   PrintInfo("BatchExecLockFile: $BatchExecLockFile");
   if ( open(fhBatchExecLockFile, ">>$BatchExecLockFile") ) {
      #-- Lock file opened
      #-- Lock the lock file
      if ( flock(fhBatchExecLockFile, LOCK_EX | LOCK_NB) ) {
         PrintInfo("BatchExecLockFile locked.");
         $RetVal = $True;
      }
      else {
         PrintInfo("Error: Cannot lock BatchExecLockFile, Batch must already be running.");
         close fhBatchExecLockFile;
      }
   }
   else {
      PrintInfo("Error: Cannot open BatchExecLockFile, Batch must already be running.");
   }
   return $RetVal;
}

##############################################################################
sub DisplayOptions
{
   PrintInfo("Options in effect:");
   PrintInfo("  -a: $opt_a") if   defined $opt_a;
   PrintInfo("  -a: Unset")  if ! defined $opt_a;

   PrintInfo("  -b: $opt_b") if   defined $opt_b;
   PrintInfo("  -b: Unset")  if ! defined $opt_b;

   PrintInfo("  -s: $opt_s") if   defined $opt_s;
   PrintInfo("  -s: Unset")  if ! defined $opt_s;

   PrintInfo("  -e: $opt_e") if   defined $opt_e;
   PrintInfo("  -e: Unset")  if ! defined $opt_e;

   PrintInfo("  -d: Set")    if   defined $opt_d;
   PrintInfo("  -d: Unset")  if ! defined $opt_d;

   PrintInfo("  -r: Set")    if   defined $opt_r;
   PrintInfo("  -r: Unset")  if ! defined $opt_r;

   PrintInfo("  -p: $opt_p") if   defined $opt_p;
   PrintInfo("  -p: Unset")  if ! defined $opt_p;

   PrintInfo("  -t: $opt_t") if   defined $opt_t;
   PrintInfo("  -t: Unset")  if ! defined $opt_t;

   PrintInfo("  -x: Set")    if   defined $opt_x;
   PrintInfo("  -x: Unset")  if ! defined $opt_x;
}

##############################################################################
#-- Main
##############################################################################

#-- Set Global Constant Variables
$True = 1;
$False = 0;
$IsUnix = $False;
if ( !($^O =~ /Win/) ) {
   $IsUnix = $True;
}
$Slash = "\\";
if ( $IsUnix ) {
   $Slash = "\/";
}

#-- Set Global Variables
$BatchNumber = "";
$RunNumber = "";
$BatchStartTime = NowDate();
$LastSuccessfulBatchNumber = "";
$LastSuccessfulRunNumber = "";
$ProcessDate = NowDate();
$LastSuccessfulProcessDate = "";
$NowEndTime = NowDate();
$ConfigurationFile = "";
$ProcessListFile = "";
$BatchType = "MANUAL";
$BatchAlias = "";
$Heartbeat = NowDate();
$TestMode = $False;
$DebugMode = $False;
$dbh = undef;
$FirstUpdateOfBatchAuditTable = $True;

$ApplicationName = "";
$BatchName = "";
$JobPollInterval = "2";
$MaxParallelJobs = "0";
$MaxArchivedLogs = 3;
$PerformAuditTableUpdates = "N";
$AuditTableUpdateInterval = 5;
$AuditTableCriticality = "WARN";
$BfConnectString = "";
$BfUserId = "";
$BfUserPassword = "";
$BfBinFileDirectory = "";
$BfLogFileDirectory = "";
$BfLockFileDirectory = "";
$BinFileDirectory = "";
$LogFileDirectory = "";
$PollFileDirectory = "";
$WorkFileDirectory = "";
$SendFailureMessage = "N";
$AlertEMailList = "";
$DomainName = domainname();
$HostUserName = getpwuid($<);

$BatchMessageLogFile ="";
$BatchAuditLogFile ="";
$BatchHistoryLogFile = "";
$ProcessAuditLogFile ="";
@ProcessList = ();
@CommentedOutProcessList = ();
%ProcessStatusList = ();
%ProcessPredecessorList = ();
%TestJobCounterList =();
%MilestoneJobCounterList =();
%ProcessFileNameList = ();
$RunOrder = 0;
$OverallProcessRunning = $False;
$OverallProcessWaiting = $False;
$OverallProcessFailed  = $False;
$BatchAuditStatus = "";
$AuditTableErrorLatch = $False;
$ResurrectMode = $False;

#-- Local Variables
my $OSRetVal = 0;
my $NumArgs = 0;
my $ResurrectFile = "";
my $BatchExecLockFile = "";

PrintInfo("Initializing ...");
PrintInfo("Job Control PID: $$");
PrintInfo("Processing options...");
if ( getopts('a:b:s:e:drp:t:xh') ) {
   #-- See if we need help
   if ( ! $opt_h ) {
      #-- No help needed
      $NumArgs = scalar(@ARGV);
      PrintInfo("Number of Parameters: $NumArgs");
      if ( $NumArgs == 1 ) {
         $ConfigurationFile = $ARGV[0];
         if ( IsValidArgs() ) {
            #-- Everything OK so far
            PrintInfo("ConfigurationFile: $ConfigurationFile");

            PrintInfo("Validating options...");
            if ( IsValidOptions() ) {
               #-- Everything OK so far
               DisplayOptions();
               #-- Turn on Debug if necessary
               if ( $opt_d ) {
                  $DebugMode = $True;
               }
               #-- Set ProcessDate if necessary
               if ( $opt_p ) {
                  $ProcessDate = $opt_p;
                  PrintInfo("ProcessDate: $ProcessDate");
               }

               #-- Bring in needed ConfigurationFile values
               PrintInfo("Processing parameter file...");
               if ( GetParameters() ) { 
                  PrintInfo("ConfigurationFile Variables:");
                  PrintInfo("   ApplicationName: $ApplicationName");
                  PrintInfo("   BatchName: $BatchName");
                  PrintInfo("   JobPollInterval: $JobPollInterval");
                  PrintInfo("   MaxParallelJobs: $MaxParallelJobs");
                  PrintInfo("   MaxArchivedLogs: $MaxArchivedLogs");
                  PrintInfo("   PerformAuditTableUpdates: $PerformAuditTableUpdates");
                  PrintInfo("   AuditTableUpdateInterval: $AuditTableUpdateInterval");
                  PrintInfo("   AuditTableCriticality: $AuditTableCriticality");
                  PrintInfo("   BfConnectString: $BfConnectString");
                  PrintInfo("   BfUserId: $BfUserId");
                  PrintInfo("   BfUserPassword: ***Protected***");
                  PrintInfo("   BfBinFileDirectory: $BfBinFileDirectory");
                  PrintInfo("   BfLogFileDirectory: $BfLogFileDirectory");
                  PrintInfo("   BfLockFileDirectory: $BfLockFileDirectory");
                  PrintInfo("   BinFileDirectory: $BinFileDirectory");
                  PrintInfo("   LogFileDirectory: $LogFileDirectory");
                  PrintInfo("   PollFileDirectory: $PollFileDirectory");
                  PrintInfo("   WorkFileDirectory: $WorkFileDirectory");
                  PrintInfo("   SendFailureMessage: $SendFailureMessage");
                  PrintInfo("   AlertEMailList: $AlertEMailList");

                  PrintInfo("Validating configuration parameters...");
                  if ( IsValidParameters() ) { 

                     #-- Set Alias
                     $BatchAlias = $BatchName;
                     if ( $opt_a ) {
                        $BatchAlias = $opt_a;
                     }

                     #-- Define batch/history/process audit log file names
                     $BatchHistoryLogFile = "$BfLogFileDirectory"."$Slash"."$BatchName"."_BatchHistory.log";
                     $BatchAuditLogFile = "$LogFileDirectory"."$Slash"."$BatchName"."_BatchAudit.log";
                     $ProcessAuditLogFile = "$LogFileDirectory"."$Slash"."$BatchName"."_ProcessAudit.log";

                     #-- Log file names
                     PrintInfo("BatchHistoryLogFile: $BatchHistoryLogFile");
                     PrintInfo("BatchAuditLogFile: $BatchAuditLogFile");
                     PrintInfo("ProcessAuditLogFile: $ProcessAuditLogFile");

                     #-- Set Process List File
                     PrintInfo("Deriving ProcessListFile...");
                     $ProcessListFile = $BinFileDirectory."/".$BatchName.".proc";
                     PrintInfo("ProcessListFile: $ProcessListFile");

                     #-- Attempt to create BatchExecLockFile
                     $BatchExecLockFile = $BfLockFileDirectory.$Slash.$ApplicationName."_".$BatchName."_BatchLock.lok";
                     if ( LockBatchExecLockFile($BatchExecLockFile) ) {
                        #-- BatchExecLockFile locked

                        #-- We are NOT resurrecting by default
                        #-- Look for Resurrect file
                        $ResurrectFile = "$PollFileDirectory".$Slash."RES.flg";
                        if ( -r $ResurrectFile ) {
                           #-- Resurrect file found
                           $ResurrectMode = $True;
                           PrintInfo("*** RES.flg detected.  Resurrecting...");
                        }

                        #-- Look for opt_r flag
                        if ( $opt_r ) {
                           #-- Resurrect option thrown
                           $ResurrectMode = $True;
                           PrintInfo("*** Resurrect option (-r) is detected.  Resurrecting...");
                        }

                        #-- See if the last run succeeded only if still attempting to resurrect
                        if ( $ResurrectMode ) {
                           if ( LastRunSucceeded() ) {
                              $ResurrectMode = $False;
                              PrintInfo("Last run succeeded.  NOT Resurrecting.");
                           }
                           else {
                              PrintInfo("Last run did NOT succeed.  Will attempt to Resurrect...");
                           }
                        }

                        #-- Only clear the work area if NOT resurrecting
                        if ( ! $ResurrectMode ) {
                           PrintInfo("Clearing work and log directories...");
                           ClearWorkArea();
                        }
                        else {
                           PrintInfo("Not clearing work and log directories.");
                        }

                        #-- Start the Batch Message log file
                        $BatchMessageLogFile = "$LogFileDirectory"."$Slash"."$BatchName"."_BatchMessage.log";
                        PrintInfo("BatchMessageLogFile: $BatchMessageLogFile");

                        #-- Delete log if it already exists
                        if ( -f "$BatchMessageLogFile" ) {
                           unlink $BatchMessageLogFile;
                        }

                        PrintInfoToBatchMessageLog("Batch message log started.");

                        #-- Log argument values
                        PrintInfoToBatchMessageLog("Parameters:");
                        PrintInfoToBatchMessageLog("   ConfigurationFile: $ConfigurationFile");

                        #-- Log derived values
                        PrintInfoToBatchMessageLog("Derived Parameters:");
                        PrintInfoToBatchMessageLog("   ProcessListFile: $ProcessListFile");

                        #-- Log parameter values
                        PrintInfoToBatchMessageLog("ConfigurationFile Variables:");
                        PrintInfoToBatchMessageLog("   ApplicationName: $ApplicationName");
                        PrintInfoToBatchMessageLog("   BatchName: $BatchName");
                        PrintInfoToBatchMessageLog("   JobPollInterval: $JobPollInterval");
                        PrintInfoToBatchMessageLog("   MaxParallelJobs: $MaxParallelJobs");
                        PrintInfoToBatchMessageLog("   MaxArchivedLogs: $MaxArchivedLogs");
                        PrintInfoToBatchMessageLog("   PerformAuditTableUpdates: $PerformAuditTableUpdates");
                        PrintInfoToBatchMessageLog("   AuditTableUpdateInterval: $AuditTableUpdateInterval");
                        PrintInfoToBatchMessageLog("   AuditTableCriticality: $AuditTableCriticality");
                        PrintInfoToBatchMessageLog("   BfConnectString: $BfConnectString");
                        PrintInfoToBatchMessageLog("   BfUserId: $BfUserId");
                        PrintInfoToBatchMessageLog("   BfUserPassword: ***Protected***");
                        PrintInfoToBatchMessageLog("   BfBinFileDirectory: $BfBinFileDirectory");
                        PrintInfoToBatchMessageLog("   BfLogFileDirectory: $BfLogFileDirectory");
                        PrintInfoToBatchMessageLog("   BfLockFileDirectory: $BfLockFileDirectory");
                        PrintInfoToBatchMessageLog("   BinFileDirectory: $BinFileDirectory");
                        PrintInfoToBatchMessageLog("   LogFileDirectory: $LogFileDirectory");
                        PrintInfoToBatchMessageLog("   PollFileDirectory: $PollFileDirectory");
                        PrintInfoToBatchMessageLog("   WorkFileDirectory: $WorkFileDirectory");
                        PrintInfoToBatchMessageLog("   SendFailureMessage: $SendFailureMessage");
                        PrintInfoToBatchMessageLog("   AlertEMailList: $AlertEMailList");

                        #-- Set the batch number - will change if resurrecting
                        PrintInfoToBatchMessageLog("Initializing BatchNumber...");
                        if ( GetUniqueBatchNumber($opt_b) ) {
                           #-- Got the next batch number
                           PrintInfoToBatchMessageLog("BatchNumber initialized.");
                           #-- Set the default RunNumber
                           $RunNumber = 1;
                           PrintInfoToBatchMessageLog("RunNumber initialized.");
                           PrintInfoToBatchMessageLog("BatchStartTime: $BatchStartTime");

                           if ( ConnectToTableMetadata() ) {
                              #-- Connection established

                              #-- Lookup last successful info
                              if ( LookupLastSuccess() ) {

                                 #-- Build Job ProcessList (2 Dim Array)
                                 #-- Reads CSV into memory
                                 PrintInfoToBatchMessageLog("Building process list...");
                                 if ( BuildProcessList() ) {

                                    #-- Build Job StatusList (Hash of Arrays)
                                    #-- Initialize memory array to WAITING values
                                    #--    Handles Resurrecting
                                    #--    Will reset BatchNumber and RunNumber for resurrecting
                                    PrintInfoToBatchMessageLog("Building status list...");
                                    if ( BuildStatusList() ) {

                                       #-- Export vars to child processes
                                       $ENV{BatchName}                 = $BatchName;
                                       $ENV{BatchNumber}               = $BatchNumber;
                                       $ENV{RunNumber}                 = $RunNumber;
                                       $ENV{ProcessDate}               = $ProcessDate;
                                       $ENV{LastSuccessfulBatchNumber} = $LastSuccessfulBatchNumber;
                                       $ENV{LastSuccessfulRunNumber}   = $LastSuccessfulRunNumber;
                                       $ENV{LastSuccessfulProcessDate} = $LastSuccessfulProcessDate;

                                       #-- Log values to log file
                                       PrintInfoToBatchMessageLog("Batch Identifiers:");
                                       PrintInfoToBatchMessageLog("   BatchName: ${BatchName}");
                                       PrintInfoToBatchMessageLog("   BatchNumber: ${BatchNumber}");
                                       PrintInfoToBatchMessageLog("   RunNumber: ${RunNumber}");
                                       PrintInfoToBatchMessageLog("   ProcessDate: $ProcessDate");
                                       PrintInfoToBatchMessageLog("   LastSuccessfulBatchNumber: $LastSuccessfulBatchNumber");
                                       PrintInfoToBatchMessageLog("   LastSuccessfulRunNumber: $LastSuccessfulRunNumber");
                                       PrintInfoToBatchMessageLog("   LastSuccessfulProcessDate: $LastSuccessfulProcessDate");

                                       #-- Build Job PredecessorList (Simple Hash)
                                       #-- Creates memory lookup structure for fast predecessor lookups
                                       #--    and checks for Deadly Embraces
                                       PrintInfoToBatchMessageLog("Building predecessor list...");
                                       if ( BuildPredecessorList() ) {

                                          #-- Build Job FileNameList (Hash of Arrays)
                                          #-- Creates memory structure of various file names
                                          PrintInfoToBatchMessageLog("Building file name list...");
                                          if ( BuildFileNameList() ) {

                                             #-- Start job control loop
                                             PrintInfoToBatchMessageLog("Starting job control loop...");
                                             $OSRetVal = ProcessLoop();
                                             PrintInfoToBatchMessageLog(
                                                "Job control loop successfully shut down with exit code: $OSRetVal");

                                             #-- One last update on various audit log
                                             PrintInfoToBatchMessageLog("About to write final update to BatchAuditLog...");
                                             if ( WriteBatchAuditLog($BatchAuditStatus) ) {
                                                PrintInfoToBatchMessageLog("About to write final update to BatchHistoryLog.");
                                                if (WriteBatchHistoryLog) {
                                                   #-- Wrote batch history log
                                                   if ( $PerformAuditTableUpdates eq "Y" ) {
                                                      PrintInfoToBatchMessageLog("About to apply final updates to Audit Tables.");
                                                      if ( ! UpdateAuditTables($BatchAuditStatus) ) {
                                                         #-- Problem writing process audit log with CRITICALITY=ERROR
                                                         $OSRetVal = 2;
                                                      }
                                                   }
                                                }
                                                else {
                                                   #-- Problem writing batch history log
                                                   $OSRetVal = 2;
                                                }
                                             }
                                             else {
                                                #-- Problem writing batch audit log
                                                $OSRetVal = 2;
                                             }

                                             #-- Copy wrk logs to log area under batch number
                                             ArchiveWorkLogs();
                                          }
                                          else {
                                             #-- Problem building FileNameList
                                             $OSRetVal = 1;
                                          }
                                       }
                                       else {
                                          #-- Problem building PredecessorList
                                          $OSRetVal = 1;
                                       }
                                    }
                                    else {
                                       #-- Problem building StatusList
                                       $OSRetVal = 1;
                                    }
                                 }
                                 else {
                                    #-- Problem building ProcessList
                                    $OSRetVal = 1;
                                 }
                              }
                              else {
                                 #-- Problem getting last dates
                                 $OSRetVal = 1;
                              }
                              #-- Disconect from MCJCMD
                              ($PerformAuditTableUpdates eq "Y") && $dbh->disconnect;
                           }
                           else {
                              #-- Problem connecting to MCJCMD
                              $OSRetVal = 1;
                           }
                        }
                        else {
                           #-- Problem getting unique batch number
                           $OSRetVal = 1;
                        }
                        PrintInfo("Batch message log stopped.");

                        #-- Unlock and remove BatchExecLockFile
                        close fhBatchExecLockFile;
                        unlink $BatchExecLockFile;
                        PrintInfo("BatchExecLockFile removed.");
                     }
                     else {
                     #-- Problem creating BatchExecLockFile
                        $OSRetVal = 1;
                     }
                  }
                  else {
                     #-- Problem validating parameters
                     $OSRetVal = 1;
                  }
               }
               else {
                  #-- Problem getting parameters
                  $OSRetVal = 1;
               }
            }
            else {
               #-- Problem with options (validation)
               $OSRetVal = 1;
            }
            PrintInfo("BatchName: $BatchName");
         }
         else {
            #-- Problem with arguments (validation)
            $OSRetVal = 1;
         }
      }

      else {
         #-- Wrong number of arguments
         PrintInfo("Error: Wrong number of command line parameters.");
         $OSRetVal = 1;
      }
   }
   else {
      #-- Help
      ShowBlurb();
      $OSRetVal = 1;
   }
}
else  {
   #-- Problem with options (getops error)
   PrintInfo("Error: Invalid options.");
   $OSRetVal = 1;
}

PrintInfo("Exiting with Return Code: $OSRetVal");
exit $OSRetVal;
