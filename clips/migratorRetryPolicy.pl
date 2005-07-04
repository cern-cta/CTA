#!/usr/bin/perl -w
#
# Default migrator retry policy. 
#
# The arguments passed to this script are:
# ARGV[1] = errorCode1
# ARGV[2] = rtcopySeverity1
# ARGV[3] = errorMessage1
# ARGV[4] = errorCode2
# ARGV[5] = rtcopySeverity2
# ARGV[6] = errorMessage2
# ...
# where errorCodeX, rtcopySeverityX and errorMessageX are the error code (serrno or errno),
# severity assigned by RTCOPY (defined in rtcp_constants.h) and the error text message (may be
# the sys_errlist[] value for the serrno/errno or a more detailed message associated with the
# error) for the Xth retry. Note that the arguments do not necessarily give the retries in
# order (X is given by the database order of the corresponding SEGMENTs).
#
# In principle a migration retry should always be possible unless the disk copy is inaccessible.
# If the disk copy is inaccessible, the filesystem is normally DISABLED so there shouldn't be
# any retry. However, in order to avoid infinite retries (with a rapidly growing ARGV to this
# script) we limit the number of retries to, say 5, by simply counting the number of arguments:
# 5 retries = 3*5 + 1 (for ARGV[0]) = 16.
# 
# The TapeErrorHandler will raise an ALERT upon a refused retry, which should normally trigger a
# manual intervention.
# 

use strict;
use diagnostics;
use POSIX;
my $doRetry = 1;

END {print "$doRetry\n";}

if ( $#ARGV > 16 ) {
	$doRetry = 0;
}

exit(EXIT_SUCCESS);
