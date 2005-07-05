#!/usr/bin/perl -w
#
# Default recaller retry policy. 
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
# the severity assigned by RTCOPY (defined in rtcp_constants.h) and the error text message (may be
# the sys_errlist[] value for the serrno/errno or a more detailed message associated with the
# error) for the Xth retry. Note that the arguments do not necessarily give the retries in
# order (X is given by the database order of the corresponding SEGMENTs).
#

use strict;
use diagnostics;
use POSIX;
my $doRetry = 1;

#
# CASTOR error constants
#
my $SEBASEOFF      = 1000;
my $SENOSHOST      = $SEBASEOFF+1;     # Host unknown                 
my $SENOSSERV      = $SEBASEOFF+2;     # Service unknown              
my $SENOTRFILE     = $SEBASEOFF+3;     # Not a remote file            
my $SETIMEDOUT     = $SEBASEOFF+4;     # Has timed out                
my $SEBADFFORM     = $SEBASEOFF+5;     # Bad fortran format specifier 
my $SEBADFOPT      = $SEBASEOFF+6;     # Bad fortran option specifier 
my $SEINCFOPT      = $SEBASEOFF+7;     # Incompatible fortran options 
my $SENAMETOOLONG  = $SEBASEOFF+8;     # File name too long           
my $SENOCONFIG     = $SEBASEOFF+9;     # Can't open configuration file
my $SEBADVERSION   = $SEBASEOFF+10;    # Version ID mismatch          
my $SEUBUF2SMALL   = $SEBASEOFF+11;    # User buffer too small        
my $SEMSGINVRNO    = $SEBASEOFF+12;    # Invalid reply number         
my $SEUMSG2LONG    = $SEBASEOFF+13;    # User message too long        
my $SEENTRYNFND    = $SEBASEOFF+14;    # Entry not found              
my $SEINTERNAL     = $SEBASEOFF+15;   # Internal error               
my $SECONNDROP     = $SEBASEOFF+16;    # Connection closed by rem. end
my $SEBADIFNAM     = $SEBASEOFF+17;    # Can't get interface name     
my $SECOMERR       = $SEBASEOFF+18;    # Communication error          
my $SENOMAPDB      = $SEBASEOFF+19;    # Can't open mapping database  
my $SENOMAPFND     = $SEBASEOFF+20;    # No user mapping              
my $SERTYEXHAUST   = $SEBASEOFF+21;    # Retry count exhausted        
my $SEOPNOTSUP     = $SEBASEOFF+22;    # Operation not supported      
my $SEWOULDBLOCK   = $SEBASEOFF+23;    # Resource temporarily unavailable 
my $SEINPROGRESS   = $SEBASEOFF+24;    # Operation now in progress    
my $SECTHREADINIT  = $SEBASEOFF+25;    # Cthread initialization error 
my $SECTHREADERR   = $SEBASEOFF+26;    # Thread interface call error  
my $SESYSERR       = $SEBASEOFF+27;    # System error                 
my $SEADNSINIT     = $SEBASEOFF+28;    # adns_init() error            
my $SEADNSSUBMIT   = $SEBASEOFF+29;    # adns_submit() error          
my $SEADNS         = $SEBASEOFF+30;    # adns resolving error         
my $SEADNSTOOMANY  = $SEBASEOFF+31;    # adns returned more than one entry 
my $SENOTADMIN     = $SEBASEOFF+32;    # requestor is not administrator 
my $SEUSERUNKN     = $SEBASEOFF+33;    # User unknown                 
my $SEDUPKEY       = $SEBASEOFF+34;    # Duplicate key value          
my $SEENTRYEXISTS  = $SEBASEOFF+35;    # Entry already exists         
my $SEGROUPUNKN    = $SEBASEOFF+36;    # Group Unknown                
my $SECHECKSUM     = $SEBASEOFF+37;    # Wrong checksum               

#
# Define some tape error constants explicitly
#
my $ETBASEOFF       = 1900;
my $ETDNP           = $ETBASEOFF+1;     # daemon not available  
my $ETSYS           = $ETBASEOFF+2;     # system error  
my $ETPRM           = $ETBASEOFF+3;     # bad parameter  
my $ETRSV           = $ETBASEOFF+4;     # reserv already issued  
my $ETNDV           = $ETBASEOFF+5;     # too many drives requested  
my $ETIDG           = $ETBASEOFF+6;     # invalid device group name  
my $ETNRS           = $ETBASEOFF+7;     # reserv not done  
my $ETIDN           = $ETBASEOFF+8;     # no drive with requested characteristics  
my $ETLBL           = $ETBASEOFF+9;     # bad label structure  
my $ETFSQ           = $ETBASEOFF+10;    # bad file sequence number  
my $ETINTR          = $ETBASEOFF+11;    # interrupted by user  
my $ETEOV           = $ETBASEOFF+12;    # EOV found in multivolume set  
my $ETRLSP          = $ETBASEOFF+13;    # release pending  
my $ETBLANK         = $ETBASEOFF+14;    # blank tape  
my $ETCOMPA         = $ETBASEOFF+15;    # compatibility problem  
my $ETHWERR         = $ETBASEOFF+16;    # device malfunction  
my $ETPARIT         = $ETBASEOFF+17;    # parity error  
my $ETUNREC         = $ETBASEOFF+18;    # unrecoverable media error  
my $ETNOSNS         = $ETBASEOFF+19;    # no sense  
my $ETRSLT          = $ETBASEOFF+20;    # reselect server  
my $ETVBSY          = $ETBASEOFF+21;    # volume busy or inaccessible  
my $ETDCA           = $ETBASEOFF+22;    # drive currently assigned  
my $ETNRDY          = $ETBASEOFF+23;    # drive not ready  
my $ETABSENT        = $ETBASEOFF+24;    # volume absent  
my $ETARCH          = $ETBASEOFF+25;    # volume archived  
my $ETHELD          = $ETBASEOFF+26;    # volume held or disabled  
my $ETNXPD          = $ETBASEOFF+27;    # file not expired  
my $ETOPAB          = $ETBASEOFF+28;    # operator cancel  
my $ETVUNKN         = $ETBASEOFF+29;    # volume unknown  
my $ETWLBL          = $ETBASEOFF+30;    # wrong label type  
my $ETWPROT         = $ETBASEOFF+31;    # cartridge write protected  
my $ETWVSN          = $ETBASEOFF+32;    # wrong vsn  
my $ETBADMIR        = $ETBASEOFF+33;    # Tape has a bad MIR 
my $ETMAXERR        = $ETBASEOFF+33;

#
# some system error codes
#
my $EPERM           = 1;      # Operation not permitted 
my $ENOENT          = 2;      # No such file or directory 
my $ESRCH           = 3;      # No such process 
my $EINTR           = 4;      # Interrupted system call 
my $EIO             = 5;      # I/O error 
my $ENXIO           = 6;      # No such device or address 
my $E2BIG           = 7;      # Arg list too long 
my $ENOEXEC         = 8;      # Exec format error 
my $EBADF           = 9;      # Bad file number 
my $ECHILD          = 10;      # No child processes 
my $EAGAIN          = 11;      # Try again 
my $ENOMEM          = 12;      # Out of memory 
my $EACCES          = 13;      # Permission denied 
my $EFAULT          = 14;      # Bad address 
my $ENOTBLK         = 15;      # Block device required 
my $EBUSY           = 16;      # Device or resource busy 
my $EEXIST          = 17;      # File exists 
my $EXDEV           = 18;      # Cross-device link 
my $ENODEV          = 19;      # No such device 
my $ENOTDIR         = 20;      # Not a directory 
my $EISDIR          = 21;      # Is a directory 
my $EINVAL          = 22;      # Invalid argument 
my $ENFILE          = 23;      # File table overflow 
my $EMFILE          = 24;      # Too many open files 
my $ENOTTY          = 25;      # Not a typewriter 
my $ETXTBSY         = 26;      # Text file busy 
my $EFBIG           = 27;      # File too large 
my $ENOSPC          = 28;      # No space left on device 
my $ESPIPE          = 29;      # Illegal seek 
my $EROFS           = 30;      # Read-only file system 
my $EMLINK          = 31;      # Too many links 
my $EPIPE           = 32;      # Broken pipe 

#
# Define the RTCOPY severity constants (rtcp_constants.h)
#
my $RTCP_OK            = 0x000001;
my $RTCP_RETRY_OK      = 0x000002;
my $RTCP_RESELECT_SERV = 0x000004;
my $RTCP_FAILED        = 0x000008;
my $RTCP_SEND_OPMSG    = 0x000010;
my $RTCP_USERR         = 0x000020;
my $RTCP_SYERR         = 0x000040;
my $RTCP_UNERR         = 0x000080;
my $RTCP_SEERR         = 0x000100;
my $RTCP_NORLS         = 0x000200;   # Don't call Ctape_rls()
my $RTCP_LOCAL_RETRY   = 0x000400;   # Do local retry 
my $RTCP_NORETRY       = 0x000800;   # Don't retry at all 
my $RTCP_EOD           = 0x001000;   # Reached End Of Data 
my $RTCP_NEXTRECORD    = 0x002000;   # Skip to next record 
my $RTCP_BLKSKPD       = 0x004000;   # A skip occured 
my $RTCP_TPE_LSZ       = 0x008000;   # A skip occured and limited by size 
my $RTCP_MNYPARY       = 0x010000;   # Request failed though  skip was attempted 
my $RTCP_LIMBYSZ       = 0x020000;   # File limited by size 
my $RTCP_ENDVOL        = 0x040000;   # End of volume hit and no more vols.


END {print "$doRetry\n";}

#
# Check for a non-retriable errors
#
open(TMP,">>/tmp/recallerRetryPolicy.out") || exit(EXIT_SUCCESS);
for ( my $i=0; $i<$#ARGV; $i+=3 ) {
    printf TMP " %d, error=%d, severity=0x%x\n",$i,$ARGV[$i],$ARGV[$i+1];
    if ( (($ARGV[$i+1] & $RTCP_RESELECT_SERV) == 0) &&
         (($ARGV[$i+1] & $RTCP_FAILED) != 0) ) {
        if ( ($ARGV[$i] != $ENOSPC) &&
             ($ARGV[$i] != $ETPARIT) &&
             ($ARGV[$i] != $ETHWERR) &&
             ($ARGV[$i] != $SETIMEDOUT) &&
             ($ARGV[$i] != $SESYSERR) &&
             ($ARGV[$i] != $SECOMERR) ) {
            $doRetry = 0;
        }
    }
}
close(TMP);

#
# Give up after 5 (5*3+1 = 16) retries
#
if ( $#ARGV > 16 ) {
	$doRetry = 0;
}

exit(EXIT_SUCCESS);
