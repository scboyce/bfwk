##############################################################################
#
# Package: comFunctions.pm
#
# Description: Common functions used by mcJobControl and any batch job
#              running under Batch Framework
#
# === Modification History ===================================================
# Date       Author           Comments
# ---------- --------------- -------------------------------------------------
# 2014-12-01 Steve Boyce     Created.
# 2016-10-07 Steve Boyce     Added IsNumber
# 2020-06-19 Steve Boyce     Added IsSignedInteger
# 2020-12-17 Steve Boyce     Added DiffDays
# 2021-03-04 Steve Boyce     Added ThisSleep
# 2021-06-25 Steve Boyce     Added GoSecond
# 2021-09-21 Steve Boyce     Updated ThisSleep
# 2021-11-01 Steve Boyce     Added GetMidSidInList
#
##############################################################################

package comFunctions;
use strict;
use File::Basename;
use Date::Pcalc qw(Add_Delta_DHMS Delta_Days Delta_DHMS Add_Delta_Days Add_Delta_YMD Today check_date);
use base "Exporter";
use JSON qw(encode_json to_json);
our @EXPORT = qw/NowDate
                 PrintInfo
                 GetBatchParameters
                 GetBatchParameter
                 StripControlChars
                 StripDblQuotes
                 Strip4ByteChar
                 Strip4ByteChars
                 Yesterday
                 DiffDays
                 DiffDhms
                 GoSecond
                 GoDay
                 GoMonth
                 GoYear
                 FirstDayOfMonth
                 LastDayOfCurrentMonth
                 LastDayOfMonth
                 LastDayOfPriorMonth
                 FifteenthOfMonth
                 FirstDayOfYear
                 FirstDayOfPriorYear
                 LastDayOfYear
                 AllTrim
                 SquashSpaces
                 StripSurroundingQuotes
                 GetFileModifyTime
                 IsNumber
                 IsInteger
                 IsSignedInteger
                 IsEmpty
                 HasWhiteSpace
                 IsYesNo
                 IsValidEmailList
                 IsValidDate
                 EscapeJson
                 ThisSleep
                 GetSimpleYamlList
                /;

##############################################################################
sub NowDate
{
   my ($InFormat) = @_;

   my $RetVal = "";
   my ($Seconds, $Minutes, $Hours, $Day, $MonthNumber, $YearNumber,
       $WeekDayNumber, $DayOfYear, $IsDayLightSavings) = localtime(time);
   my $Year = $YearNumber + 1900;
   my $Month = sprintf("%02d", $MonthNumber + 1);
   my $MonthName = "";

   $Day = sprintf("%02d", $Day);
   $Hours = sprintf("%02d", $Hours);
   $Minutes = sprintf("%02d", $Minutes);
   $Seconds = sprintf("%02d", $Seconds);

   #-- Default date return format
   $RetVal = "$Year-$Month-$Day $Hours:$Minutes:$Seconds";

   #-- Format the date differently if a format was passed
   if ( defined($InFormat) ) {
      if    ( $InFormat eq "YYYYMMDD" )               { $RetVal = "$Year$Month$Day"; }
      elsif ( $InFormat eq "YYYY-MM-DD" )             { $RetVal = "$Year-$Month-$Day"; }
      elsif ( $InFormat eq "YYYY" )                   { $RetVal = "$Year"; }
      elsif ( $InFormat eq "DDMMYYYY" )               { $RetVal = "$Day$Month$Year"; }
      elsif ( $InFormat eq "DD-MM-YYYY" )             { $RetVal = "$Day-$Month-$Year"; }
      elsif ( $InFormat eq "YYYY-MM-DDTHH24:MI:SS" )  { $RetVal = "$Year-$Month-$Day"."T"."$Hours:$Minutes:$Seconds"; }
      elsif ( $InFormat eq "YYYYMMDDHH24MISS" )       { $RetVal = "$Year$Month$Day$Hours$Minutes$Seconds"; }
      elsif ( $InFormat eq "YYYYMMDD.HH24MISS" )      { $RetVal = "$Year$Month$Day.$Hours$Minutes$Seconds"; }
      elsif ( $InFormat eq "DD-MMM-YYYY HH24:MI:SS" ) {
         SWITCH: {
            if ( "$Month" eq "01" ) { $MonthName = "Jan"; last SWITCH; }
            if ( "$Month" eq "02" ) { $MonthName = "Feb"; last SWITCH; }
            if ( "$Month" eq "03" ) { $MonthName = "Mar"; last SWITCH; }
            if ( "$Month" eq "04" ) { $MonthName = "Apr"; last SWITCH; }
            if ( "$Month" eq "05" ) { $MonthName = "May"; last SWITCH; }
            if ( "$Month" eq "06" ) { $MonthName = "Jun"; last SWITCH; }
            if ( "$Month" eq "07" ) { $MonthName = "Jul"; last SWITCH; }
            if ( "$Month" eq "08" ) { $MonthName = "Aug"; last SWITCH; }
            if ( "$Month" eq "09" ) { $MonthName = "Sep"; last SWITCH; }
            if ( "$Month" eq "10" ) { $MonthName = "Oct"; last SWITCH; }
            if ( "$Month" eq "11" ) { $MonthName = "Nov"; last SWITCH; }
            $MonthName = "Dec";
         }
         $RetVal = "$Day-$MonthName-$Year $Hours:$Minutes:$Seconds";
      }
   }
   return $RetVal;
}

##############################################################################
sub PrintInfo
{
   my ($MessageLine) = @_;
   my ($Package, $FileName, $Line) = caller;
   print NowDate(), " ", basename($FileName),":$Line", " ", $MessageLine, "\n";
}

##############################################################################
sub GetBatchParameters
{
   my ($ConfFile) = @_;
   my %RetHash;

   if ( -r $ConfFile ) {
      if ( open( fhConfigFile, "<", $ConfFile) ) {
         while ( <fhConfigFile> ) {
            chomp;
            if ( m/^\s*(\w+)\="?\s*(.*?)\s*"?\s*$/ ) {
               $RetHash{ $1 } = "$2";
            }
         }
         close fhConfigFile;
      }
   }
   else {
      die PrintInfo("Error: Unable to open ConfigFile: $ConfFile");
   }
   return \%RetHash
}

##############################################################################
sub GetBatchParameter
{
   my ($BatchParamHashRef, $ParamName, $Required, $ShowIt) = @_;
   #-- Required
   #--    0 - OK if missing
   #--    1 - Die if missing
   #-- ShowIt
   #--    0 - Don't show it at all
   #--    1 - Disply value
   #--    2 - Display masked value

   my $RetVal;
   $RetVal = $BatchParamHashRef->{"$ParamName"};
   if ( !IsEmpty($RetVal) ) {
      #-- Found value
      #-- Display something
      if ( $ShowIt ) {
         #-- Display something
         if ( $ShowIt == 1 ) {
            #-- Display value
            PrintInfo("PARAMETER: $ParamName: [$RetVal]");
         }
         elsif ( $ShowIt == 2 ) {
            #-- Display Masked value
            PrintInfo("PARAMETER: $ParamName: *** Protected ***");
         }
         else {
            die PrintInfo("Error: Invalid ShowIt ($ShowIt) parameter.")
         }
      }
   }
   else {
      #-- Missing value
      if ( $Required ) {
         #-- Trigger error
         die PrintInfo("Error: Parameter ($ParamName) is required.");
      }
      else {
         PrintInfo("PARAMETER: $ParamName: <EMPTY>");
      }
   }
   return $RetVal
}

##############################################################################
sub StripControlChars
{
   my ($String) = @_;

   #-- OCT DEC
   #-- --- ---
   #-- 000 000
   #-- 001 001
   #-- 002 002
   #-- 003 003
   #-- 004 004
   #-- 005 005
   #-- 006 006
   #-- 007 007
   #-- 010 008
   #-- 011 009
   #-- 012 010
   #-- 013 011
   #-- 014 012
   #-- 015 013
   #-- 016 014
   #-- 017 015
   #-- 020 016
   #-- 021 017
   #-- 022 018
   #-- 023 019
   #-- 024 020
   #-- 025 021
   #-- 026 022
   #-- 027 023
   #-- 030 024
   #-- 031 025
   #-- 032 026
   #-- 033 027
   #-- 034 028
   #-- 035 029
   #-- 036 030
   #-- 037 031
   #-- 174 124

   #-- Codes are Octal - Why did I include pipes (174/124)?
   $String =~ s/[\000\001\002\003\004\005\006\007\010\011\012\013\014\015\016\017\020\021\022\023\024\025\026\027\030\031\032\033\034\035\036\037\174]//g;
   return $String;
}

##############################################################################
sub StripDblQuotes
{
   my ($String) = @_;
   my $Length = 0;
   my $RetVal = "";

   $Length = length($String);
   if ( substr($String, 0, 1) eq '"' && substr($String, $Length-1, 1) eq '"' ) {
      $RetVal = substr($String, 1, $Length - 2);
   }
   else {
      $RetVal = $String;
   }
   return $RetVal;
}

##############################################################################
sub Strip4ByteChar
{

   my ($String) = @_;
   my $RetVal = "";

   my $LengthOfInputLine = length($String);
   my $CurColPos = 0;
   my $InCharacter;
   my $DecimalCharCode;
   while ($CurColPos < $LengthOfInputLine) {

      $InCharacter = substr($String, $CurColPos, 1);
      $DecimalCharCode = ord($InCharacter);

      if ( $DecimalCharCode <= 65535 ) {
         $RetVal = $RetVal . $InCharacter;
      }
      else {
         #-- Replace with Unicode Unk Character
         $RetVal = $RetVal . chr(0xFFFD);
      }
      $CurColPos++;
   }
   return $RetVal;

}

##############################################################################
sub Strip4ByteChars
{
   #-- Mode (optional):
   #-- R - Replace with std utf8 replacement character
   #-- S - Strip (Default)

   my ($String, $Mode) = @_;
   my $RetVal = "";

   my $LengthOfInputLine = length($String);
   my $CurColPos = 0;
   my $InCharacter;
   my $DecimalCharCode;
   while ($CurColPos < $LengthOfInputLine) {

      $InCharacter = substr($String, $CurColPos, 1);
      $DecimalCharCode = ord($InCharacter);

      if ( $DecimalCharCode <= 65535 ) {
         $RetVal = $RetVal . $InCharacter;
      }
      else {
         if ( "$Mode" eq "R" ) {
            #-- Replace with Unicode Unk Character
            $RetVal = $RetVal . chr(0xFFFD);
         }
      }
      $CurColPos++;
   }
   return $RetVal;

}

##############################################################################
sub Yesterday
{
   my $Year;
   my $Month;
   my $Day;
   my $RetVal;

   ($Year, $Month, $Day) = Today();
   ($Year, $Month, $Day) = Add_Delta_Days($Year, $Month, $Day, -1);
   $RetVal = $Year."-".sprintf("%02d", $Month)."-".sprintf("%02d", $Day);
}

##############################################################################
sub DiffDays
{
   #-- Wrapper for Delta_Days
   #-- Deals with converting from/to "YYYY-MM-DD" format
   #-- FromDate and ToDate format must be YYYY-MM-DD
   my ($FromDate, $ToDate) = @_;
   my $RetVal = "";
   my $FromYear  = substr($FromDate, 0, 4);
   my $FromMonth = substr($FromDate, 5, 2);
   my $FromDay   = substr($FromDate, 8, 2);
   my $ToYear    = substr($ToDate, 0, 4);
   my $ToMonth   = substr($ToDate, 5, 2);
   my $ToDay     = substr($ToDate, 8, 2);

   $RetVal = Delta_Days($FromYear, $FromMonth, $FromDay,
                        $ToYear, $ToMonth, $ToDay
                       );

   return $RetVal;
}

##############################################################################
sub DiffDhms {

   #-- Wrapper for Delta_DHMS
   my ($DateTime1, $DateTime2) = @_;
   my $Days;
   my $Hours;
   my $minutes;
   my $Seconds;
   my $RetVal = "";

   #-- 0         1
   #-- 0123456789012345678
   #-- YYYY-MM-DD HH:MM:SS

   my $Year1  = substr($DateTime1,  0, 4);
   my $Month1 = substr($DateTime1,  5, 2);
   my $Day1   = substr($DateTime1,  8, 2);
   my $Hour1  = substr($DateTime1, 11, 2);
   my $Min1   = substr($DateTime1, 14, 2);
   my $Sec1   = substr($DateTime1, 17, 2);

   my $Year2  = substr($DateTime2, 0, 4);
   my $Month2 = substr($DateTime2, 5, 2);
   my $Day2   = substr($DateTime2, 8, 2);
   my $Hour2  = substr($DateTime2, 11, 2);
   my $Min2   = substr($DateTime2, 14, 2);
   my $Sec2   = substr($DateTime2, 17, 2);

   ($Days, $Hours, $minutes, $Seconds) =
      Delta_DHMS($Year1, $Month1, $Day1, $Hour1, $Min1, $Sec1,
                 $Year2, $Month2, $Day2, $Hour2, $Min2, $Sec2
                       );
   $RetVal = "$Days:$Hours:$minutes:$Seconds";

   return $RetVal;
}

##############################################################################
sub GoSecond
{
   #-- Add_Delta_DHMS($year,$month,$day, $hour,$min,$sec, $Dd,$Dh,$Dm,$Ds);

   #-- Wrapper for Add_Delta_DHMS
   #-- Deals with converting from/to "YYYY-MM-DD HH:MI:SS" format
   #-- InPeriod format must be YYYY-MM-DD HH:MI:SS
   my ($InPeriod, $Offset) = @_;
   my $RetVal = "";
   my $InYear  = substr($InPeriod, 0, 4);
   my $InMonth = substr($InPeriod, 5, 2);
   my $InDay   = substr($InPeriod, 8, 2);
   my $InHour  = substr($InPeriod, 11, 2);
   my $InMin   = substr($InPeriod, 14, 2);
   my $InSec   = substr($InPeriod, 17, 2);

   my ($Year, $Month, $Day, $Hour, $Min, $Sec) = Add_Delta_DHMS($InYear, $InMonth, $InDay,
                                                                $InHour, $InMin, $InSec,
                                                                0, 0, 0, $Offset);
   $RetVal = "$Year-".sprintf("%02d", $Month)."-".sprintf("%02d", $Day).
             " ".sprintf("%02d", $Hour).":".sprintf("%02d", $Min).":".sprintf("%02d", $Sec);

   #-- RetVal format: YYYY-MM-DD HH:MI:SS
   return $RetVal;
}

##############################################################################
sub GoDay
{
   #-- Wrapper for Add_Delta_Days
   #-- Deals with converting from/to "YYYY-MM-DD" format
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod, $Offset) = @_;
   my $RetVal = "";
   my $Year = substr($InPeriod, 0, 4);
   my $Month = substr($InPeriod, 5, 2);
   my $Day = substr($InPeriod, 8, 2);

   ($Year, $Month, $Day) = Add_Delta_Days($Year, $Month, $Day,
                                          $Offset);
   $RetVal = "$Year-".sprintf("%02d", $Month)."-".sprintf("%02d", $Day);

   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub GoMonth
{
   #-- Wrapper for Add_Delta_YMD
   #-- Deals with converting from/to "YYYY-MM-DD" format
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod, $Offset) = @_;
   my $RetVal = "";
   my $Year = substr($InPeriod, 0, 4);
   my $Month = substr($InPeriod, 5, 2);
   my $Day = substr($InPeriod, 8, 2);

   ($Year, $Month, $Day) = Add_Delta_YMD($Year, $Month, $Day,
                                         0, $Offset, 0);
   $RetVal = "$Year-".sprintf("%02d", $Month)."-".sprintf("%02d", $Day);

   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub GoYear
{
   #-- Wrapper for Add_Delta_YMD
   #-- Deals with converting from/to "YYYY-MM-DD" format
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod, $Offset) = @_;
   my $RetVal = "";
   my $Year = substr($InPeriod, 0, 4);
   my $Month = substr($InPeriod, 5, 2);
   my $Day = substr($InPeriod, 8, 2);

   ($Year, $Month, $Day) = Add_Delta_YMD($Year, $Month, $Day,
                                         $Offset, 0, 0);
   $RetVal = "$Year-".sprintf("%02d", $Month)."-".sprintf("%02d", $Day);

   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub FirstDayOfMonth
{
   #-- Deals with converting from/to "YYYY-MM-DD" format
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod) = @_;
   my $RetVal = "";
   my $Year = substr($InPeriod, 0, 4);
   my $Month = substr($InPeriod, 5, 2);

   $RetVal = "$Year-".sprintf("%02d", $Month)."-01";
   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub LastDayOfCurrentMonth
{
   my $Year;
   my $Month;
   my $Day;
   my $RetVal;

   ($Year, $Month, $Day) = Today();
   if ( $Month > 11 ) {
      $Year++;
      $Month = 1;
      $Day = 1;
   }
   else {
      $Month++;
      $Day = 1;
   }
   ($Year, $Month, $Day) = Add_Delta_Days($Year, $Month, $Day, -1);
   $RetVal = $Year."-".sprintf("%02d", $Month)."-".sprintf("%02d", $Day);
}

##############################################################################
sub LastDayOfMonth
{
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod) = @_;
   my $RetVal = GoDay(FirstDayOfMonth(GoMonth($InPeriod, 1)), -1);

   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub LastDayOfPriorMonth
{
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod) = @_;

   my $Year = substr($InPeriod, 0, 4);
   my $Month = substr($InPeriod, 5, 2);
   my $Day = 1;
   my $RetVal;

   ($Year, $Month, $Day) = Add_Delta_Days($Year, $Month, $Day, -1);
   $RetVal = $Year."-".sprintf("%02d", $Month)."-".sprintf("%02d", $Day);
}

##############################################################################
sub FifteenthOfMonth
{
   #-- Deals with converting from/to "YYYY-MM-DD" format
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod) = @_;
   my $RetVal = "";
   my $Year = substr($InPeriod, 0, 4);
   my $Month = substr($InPeriod, 5, 2);

   $RetVal = "$Year-".sprintf("%02d", $Month)."-15";

   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub FirstDayOfYear
{
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod) = @_;
   my $Year = substr($InPeriod, 0, 4);
   my $RetVal = "$Year"."-01-01";

   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub FirstDayOfPriorYear
{
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod) = @_;
   my $Year = substr($InPeriod, 0, 4) -1;
   my $RetVal = sprintf("%04d", $Year)."-01-01";

   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub LastDayOfYear
{
   #-- InPeriod format must be YYYY-MM-DD
   my ($InPeriod) = @_;
   my $Year = substr($InPeriod, 0, 4);
   my $RetVal = "$Year"."-12-31";

   #-- RetVal format: YYYY-MM-DD
   return $RetVal;
}

##############################################################################
sub AllTrim
{
   my ($String) = @_;
   my $RetVal = $String;

   if ( defined($RetVal) ) {
      $RetVal =~ s/^ *//;
      $RetVal =~ s/ *$//;
   }
   else {
      $RetVal = "";
   }
   return $RetVal;
}

##############################################################################
sub SquashSpaces
{
   my ($String) = @_;
   my $RetVal = $String;

   if ( $RetVal ) {
      $RetVal =~ tr/ / /s;
   }
   else {
      $RetVal = "";
   }
   return $RetVal;
}

##############################################################################
sub StripSurroundingQuotes
{
   my ($String) = @_;
   my $RetVal = $String;
   my $StringLen = length($String);

   if ( $StringLen > 1 ) {
      my $FirstChar = substr($String, 0, 1);
      my $LastChar = substr($String, $StringLen-1, 1);

      if ( ("$FirstChar" eq "\"" && "$LastChar" eq "\"") || ("$FirstChar" eq "\'" && "$LastChar" eq "\'") ) {
         #-- String is surrounded by double or single quotes
         $RetVal = substr($String, 1, $StringLen-2 );
      }
   }
   return $RetVal;
}

##############################################################################
sub GetFileModifyTime
{
   my ($File) = @_;
   my $RetVal;

   my ($dev, $ino, $mode, $nlink, $uid, $gid, $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks) = stat($File);

   my ($Seconds, $Minutes, $Hours, $Day, $MonthNumber, $YearNumber,
       $WeekDayNumber, $DayOfYear, $IsDayLightSavings) = localtime($mtime);

   my $Year = $YearNumber + 1900;
   my $Month = sprintf("%02d", $MonthNumber + 1);

   $Day = sprintf("%02d", $Day);
   $Hours = sprintf("%02d", $Hours);
   $Minutes = sprintf("%02d", $Minutes);
   $Seconds = sprintf("%02d", $Seconds);
   $RetVal = "$Year-$Month-$Day $Hours:$Minutes:$Seconds";
   return $RetVal;
}

##############################################################################
sub IsNumber
{
   my ($String) = @_;
   my $RetVal;

   if ( $String =~ m/^([+-]?)(?=\d|\.\d)\d*(\.\d*)?$/ ) {
      $RetVal = 1;
   }
   else {
      $RetVal = 0;
   }
   return $RetVal;
}

##############################################################################
sub IsInteger
{
   my ($String) = @_;
   my $RetVal;

   if ( $String =~ m/^\d*?$/ ) {
      $RetVal = 1;
   }
   else {
      $RetVal = 0;
   }
   return $RetVal;
}

##############################################################################
sub IsSignedInteger
{
   my ($String) = @_;
   my $RetVal;

   if ( $String =~ m/^([+-]?)\d*?$/ ) {
      $RetVal = 1;
   }
   else {
      $RetVal = 0;
   }
   return $RetVal;
}

##############################################################################
sub IsEmpty
{
   my ($String) = @_;
   my $RetVal;

   if ( $String =~ m /^\s*$/ ) {
      $RetVal = 1;
   }
   else {
      $RetVal = 0;
   }
   return $RetVal;
}

##############################################################################
sub HasWhiteSpace {
   my ($InStr) = @_;
   if ( $InStr =~ /\s+/ ) {
      return 1;
   }
   else {
      return 0;
   }
}

##############################################################################
sub IsYesNo {
   my ($InStr) = @_;
   if (uc($InStr) eq "Y" || uc($InStr) eq "YES" ||
       uc($InStr) eq "N" || uc($InStr) eq "NO" ) {
      return 1;
   }
   else {
      return 0;
   }
}

##############################################################################
sub IsValidEmailList {
   my ($InStr) = @_;
   if ($InStr =~ /^\s*([A-Z0-9._%+-]+@([A-Z0-9-]+\.)+[A-Z]{2,4} *, *)*[A-Z0-9._%+-]+@([A-Z0-9-]+\.)+[A-Z]{2,4}+\s*$/i ) {
      return 1;
   }
   else {
      return 0;
   }
}

##############################################################################
sub IsValidDate
{
   #-- Deals with converting from "YYYY-MM-DD" format
   #-- InDate format must be YYYY-MM-DD
   my ($InDate) = @_;
   my ($Year, $Month, $Day) = split("-", $InDate);

   return check_date($Year, $Month, $Day);
}

##############################################################################
sub EscapeJson
{
   #-- This function will escape a string so that is suitable for embedding
   #-- into a JSON value.
   my ($InStr) = @_;

   #-- Remove leading/trailing whitespace
   $InStr = AllTrim($InStr);

   #-- Load data into a Hash
   my %Json_Hash = ( 'string' , $InStr);

   #-- JSON encode it and strip away the JSON string stub
   my $OutStr = substr(to_json(\%Json_Hash), 11, -2);

   return $OutStr;
}

##############################################################################
sub ThisSleep
{
   #-- This function will return the indexed sleep value from the sleep
   #--    list based on the current retry attempt.
   #-- The calling code will only use this when implemeting retry logic
   #--    The first attempt in the controlling loop is not considered a retry.
   #--    The second attempt in the controlling loop is considered the first retry attempt.
   #-- $RetrySleepList is a comma separated list of retry sleep times
   #--    i.e. "10,20,30,90,180,60"
   #--    It can be one value or any arbitrary number of values.
   #-- $RetryAttempt is the current retry attempt iteration number (1-n)
   #--    It assumes that the 1st retry iteration will use the first value and the
   #--    2nd iteration will use the second value etc, until it runs of out values
   #--    in which case it will conitue to return the last value in the list.
   #--
   #-- Example usage:
   #--    ------------------------------------------------------------------
   #--    my $MaxRetryAttempts = 10;
   #--    my $RetrySleepList = "10,20,30,90,180,60";
   #--    my $CurrentAttempt = 1;
   #--    my $RetryAttempt = 0;
   #--    my $SleepSeconds;
   #--    while ( 1 ) {
   #--       PrintInfo("Doing something (attempt: $CurrentAttempt...");
   #--       if ( SomeThing() ) {
   #--          #-- Success
   #--          last;
   #--       else {
   #--          #-- Failure
   #--          $CurrentAttempt++;
   #--          $RetryAttempt++;
   #--          if ( $RetryAttempt <= $MaxRetryAttempts ) {
   #--             #-- Retry
   #--             $SleepSeconds = ThisSleep($RetrySleepList, $RetryAttempt);
   #--             PrintInfo("Sleeping for $ApiRetrySleep seconds...");
   #--             sleep $SleepSeconds;
   #--          }
   #--          else {
   #--             #-- Give up
   #--             PrintInfo("Retry attempts exhausted, giving up.");
   #--          last;
   #--          }
   #--       }
   #--    }
   #--    ------------------------------------------------------------------
   #-- MaxRetryAttempts and RetrySleepList is best defined in the Batch config file.
   #-- The controlling loop will keep track of attempts and retrys and pass retrys
   #--    as the attempt retry value.

   my ($RetrySleepList, $RetryAttempt) = @_;

   ( $RetrySleepList )          or die PrintInfo("Error: RetrySleepList is empty.");
   ( IsInteger($RetryAttempt) ) or die PrintInfo("Error: RetryAttempt must be an integer.");
   ( $RetryAttempt > 0 )        or die PrintInfo("Error: RetryAttempt must be greater than zero.");

   #-- Default retry list to std list
   $RetrySleepList = $RetrySleepList ? $RetrySleepList : "10,20,30";

   #-- Parse list into array... assume comma separated list
   my (@SleepList) = split(/\,/, $RetrySleepList);

   #-- Grab the max index of the array
   my $MaxIndex = $#SleepList;

   #-- Calculate how long to sleep
   my $ThisSleep;
   if ( $RetryAttempt >= $MaxIndex+1 ) {
      $ThisSleep = $SleepList[$MaxIndex];
   }
   else {
      $ThisSleep = $SleepList[$RetryAttempt-1];
   }
   return $ThisSleep;
}

##############################################################################
sub GetSimpleYamlList
{

   my ($YamlFile, $YamlKey, $QuoteIt) = @_;

   #-- Bring in YAML Config items
   my $YamlList = YAML::Tiny->read($YamlFile);
   ($YamlList) or die PrintInfo("Error: Unable to parse YAML file.");

   #-- Point to list of items
   my $YamlArrayRef = $YamlList->[0]->{$YamlKey};
   #print Dumper($YamlArrayRef), "\n";

   #-- Build List
   my $List;
   if ( $QuoteIt ) {
      #-- Add single quotes to each item in list
      foreach my $Entry (@{$YamlArrayRef}) {
         #print "--> $Entry", "\n";
         $List = $List . "'".$Entry."',";
      }
      chop $List;
   }
   else {
      #-- Join raw
      $List = join(",", @{$YamlArrayRef});
   }
   #PrintInfo("List: $List");
   ($List) or die PrintInfo("Error: Empty Yaml list.");

   return ($List);

}

##############################################################################
1;
