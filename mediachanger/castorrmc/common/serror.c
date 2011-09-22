/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* serror.c     Global error reporting routines                         */

#include <stdio.h>      /* standard input/output                        */
#include <errno.h>      /* error numbers and codes                      */

#include <serrno.h>     /* special error numbers and codes              */
#include <log.h>        /* logger functions                             */
#include <Cglobals.h>
#include <string.h>

#if !defined(linux) && !defined(__APPLE__) 
extern int      sys_nerr;       /* number of system error messages      */
#endif

#include <net.h>                /*     networking specifics             */

char    *sys_serrlist[SEMAXERR-SEBASEOFF+2]=
  {"Error 0",
   "Host not known",
   "Service unknown",
   "Not a remote file",
   "Timed out",
   "Unsupported FORTRAN format",
   "Unknown FORTRAN option",
   "Incompatible FORTRAN options",
   "File name too long",
   "Can't open configuration file",
   "Version ID mismatch",
   "User buffer too small",
   "Invalid reply number",
   "User message too long",
   "Entry not found",
   "Internal error",
   "Connection closed by remote end",
   "Can't find interface name",
   "Communication error",
   "Can't open mapping database",
   "No user mapping",
   "Retry count exhausted",
   "Operation not supported",
   "Resource temporarily unavailable",
   "Operation now in progress",
   "Cthread initialization error",
   "Thread interface call error",
   "System error",
   "deprecated error",
   "deprecated error",
   "deprecated error",
   "deprecated error",
   "requestor is not administrator",
   "User unknown",
   "Duplicate key value",
   "Entry already exists",
   "Group unknown",
   "Bad checksum",
   "This service class is not available for this host",
   "Got SQL exception from database",
   "Too many symbolic links",
   "No port in range",
   "No value",
   "Invalid configuration",
   "Failed to execute Python",
   "BAD ERROR NUMBER"
  };

/*
 * Package specific error messages (don't forget to update h/serrno.h)
 */

/*
 *------------------------------------------------------------------------
 * COPYTAPE specific error messages
 *------------------------------------------------------------------------
 */
char *sys_cerrlist[ECMAXERR-ECBASEOFF+2] =
  {"Error 0",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * DB specific error messages
 *------------------------------------------------------------------------
 */
char *sys_dberrlist[EDBMAXERR-EDBBASEOFF+2] =
  {"Error 0",
   "Cdb api : invalid Cdb_sess_t parameter",
   "Cdb api : invalid Cdb_db_t parameter",
   "Cdb api : invalid value",
   "Cdb api : Cdb server hostname resolution error (Cgethostbyname)",
   "Cdb api : data size rejected (exceeds theorical value)",
   "Cdb api system : socket() error",
   "Cdb api system : [set/get]sockopt() error",
   "Cdb api system : malloc() error",
   "Cdb api        : no last error",
   "Cdb api : interface invalid value",
   "Cdb api system : bind() error",
   "Cdb api system : listen() error",
   "Cdb api system : getsockname() error",
   "Cdb api system : accept() error",
   "Cdb api system : getpeername() error",
   "Cdb api : connection back from bad host",
   "Error 17",
   "Error 18",
   "Error 19",
   "Cdb daemon : invalid value",
   "Cdb daemon : yet done",
   "Cdb daemon : unauthorized",
   "Cdb daemon : login refused",
   "Cdb daemon : password file corrupted",
   "Cdb daemon : database description analysis error",
   "Cdb daemon : bad hash size",
   "Cdb daemon : unknown database/table/key",
   "Cdb daemon : a lock is required",
   "Cdb daemon : datatabase (probably) corrupted",
   "Cdb daemon : data size rejected (exceeds theorical value)",
   "Cdb daemon : no entry",
   "Cdb daemon : unknown record member type",
   "Cdb daemon : unknown record member val",
   "Cdb daemon : null record member value",
   "Cdb daemon : lock deny",
   "Cdb daemon : attempt to free something Cdb claims it is of zero size",
   "Cdb daemon : shutdown in progress",
   "Cdb daemon : deadlock detected",
   "Cdb daemon : yet exists",
   "Cdb daemon : no more space",
   "Cdb daemon : end of dump",
   "Cdb daemon : uniqued key yet exists",
   "Cdb daemon : end of list",
   "Cdb daemon : not in dump mode",
   "Cdb daemon : double DNS check error",
   "Cdb daemon : Connection rejected (not authorised)",
   "Cdb daemon : init in progress",
   "Cdb daemon : inconsistent request (unstop and no previous stop, unfreeze and no previous freeze)",
   "Cdb daemon : bad free hash size",
   "Cdb daemon system : malloc() error",
   "Cdb daemon system : calloc() error",
   "Cdb daemon system : realloc() error",
   "Cdb daemon system : open() error",
   "Cdb daemon system : fstat() error",
   "Cdb daemon system : lseek() error",
   "Cdb daemon system : read() error",
   "Cdb daemon system : write() error",
   "Cdb daemon system : rename() error",
   "Cdb daemon system : ftruncate() error",
   "Cdb daemon system : tmpnam() error",
   "Cdb daemon system : fcntl() error",
   "Cdb daemon system : mkdir() error",
   "Cdb daemon system : times() error",
   "Cdb daemon system : sysconf() err/unav",
   "Cdb daemon system : Cdb client hostname resolution error (Cgethostbyname)",
   "Cdb daemon system : getpeername() error",
   "Cdb daemon system : inet_ntoa() error",
   "Cdb daemon system : remove() error",
   "Cdb daemon system : sigaction() error",
   "Cdb daemon system : getsockname() error",
   "Cdb daemon system : bind() error",
   "Cdb daemon system : listen() error",
   "Cdb daemon system : connect() error",
   "Cdb daemon system : socket() error",
   "Cdb daemon system : [set/get]socketopt() error",
   "Cdb daemon : Host resolution error",
   "Cdb daemon : Request too big",
   "Error 78",
   "Error 79",
   "Cdb config : invalid configuration value",
   "Cdb config : configuration error",
   "Cdb config : configuration value rejected (exceeds maximum size)",
   "Cdb config system : gethostname() error",
   "Error 84",
   "Error 85",
   "Error 86",
   "Error 87",
   "Error 88",
   "Error 89",
   "Cdb : nomoredb",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * MSG daemon specific error messages
 *------------------------------------------------------------------------
 */
char *sys_mserrlist[EMSMAXERR-EMSBASEOFF+2] =
  {"Error 0",
   "Message daemon unable to reply",
   "Message daemon system error",
   "Permission denied",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * NS (Name Server) specific error messages
 *------------------------------------------------------------------------
 */
char *sys_nserrlist[ENSMAXERR-ENSBASEOFF+2] =
  {"Error 0",
   "Name server not active",
   "File has been overwritten, request ignored",
   "Segment had been deleted",
   "Is a link",
   "File class does not allow a copy on tape",
   "Too many copies on tape",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * RFIO specific error messages
 *------------------------------------------------------------------------
 */
char *sys_rferrlist[ERFMAXERR-ERFBASEOFF+2] =
  {"Error 0",
   "Host did not return error number",
   "Host is not on local network and no mapping found",
   "Cannot create linked file across hosts",
   "Protocol not supported",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * RTCOPY specific error messages
 *------------------------------------------------------------------------
 */
char *sys_rterrlist[ERTMAXERR-ERTBASEOFF+2] =
  {"Error 0",
   "TMS error",
   "Blocks skipped in file",
   "Blocks skipped and file limited by size",
   "Too many skipped blocks",
   "File limited by size",
   "Request interrupted by user",
   "Request interrupted by operator",
   "Request list is not circular",
   "Bad request structure",
   "Request partially processed",
   "Catalogue DB error",
   "Zero sized file",
   "Recalled file size incorrect",
   "Inconsistent FSEQ in VMGR and Cns",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * STAGE specific error messages
 *------------------------------------------------------------------------
 */
char *sys_sterrlist[ESTMAXERR-ESTBASEOFF+2] =
  {"Error 0",
   "Aborted",
   "Enough free space",
   "Symbolic link not created",
   "Symbolic link not supported",
   "Stager not active",
   "Your group is invalid",
   "No GRPUSER in configuration",
   "Invalid user",
   "HSM HOST not specified",
   "tmscheck error",
   "User link name processing error",
   "User path in a non-writable directory",
   "Request killed",
   "Request too long (socket buffer size)",
   "Stage configuration error",
   "Unreadable file on tape (segments not all accessible)",
   "File replication failed",
   "File is currently not available",
   "Job killed by service administrator",
   "Job timed out while waiting to be scheduled",
   "Scheduler error",
   "No filesystems available in service class",
   "File has no copy on tape and no diskcopies are accessible",
   "File is on an offline tape",
   "Request canceled while queuing",
   "Tape-copy not found",
   "The file cannot be routed to tape",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * SYSREQ specific error messages
 *------------------------------------------------------------------------
 */
char *sys_sqerrlist[ESQMAXERR-ESQBASEOFF+2] =
  {"Error 0",
   "TMS not active",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * TAPE specific error messages
 *------------------------------------------------------------------------
 */
char *sys_terrlist[ETMAXERR-ETBASEOFF+2] =
  {"Error 0",
   "Tape daemon not available",
   "System error",
   "Bad parameter",
   "Reserve already issued",
   "Too many drives requested",
   "Invalid device group name",
   "Reserve not done",
   "No drive with requested name/characteristics",
   "Bad label structure",
   "Bad file sequence number",
   "Interrupted by user",
   "EOV found in multivolume set",
   "Release pending",
   "Blank tape",
   "Compatibility problem",
   "Device malfunction",
   "Parity error",
   "Unrecoverable media error",
   "No sense",
   "Reselect server",
   "Volume busy or inaccessible",
   "Drive currently assigned",
   "Drive not ready",
   "Volume absent",
   "Volume archived",
   "Volume held or disabled",
   "File not expired",
   "Operator cancel",
   "Volume unknown",
   "Wrong label type",
   "Cartridge write protected",
   "Wrong vsn",
   "Tape has a bad MIR",
   "castor::tape::net::acceptConnection interrupted",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * VAL (Volume Allocator) specific error messages
 *------------------------------------------------------------------------
 */
char *sys_vaerrlist[EVMMAXERR-EVMBASEOFF+2] =
  {"Error 0",
   "Volume manager not active",
   "VMGR HOST not set",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * UPV (User Privilege Validator) specific error messages
 *------------------------------------------------------------------------
 */
char *sys_uperrlist[EUPMAXERR-EUPBASEOFF+2] =
  {"Error 0",
   "UPV not active",
   "BAD ERROR NUMBER"
  };


/*
 *------------------------------------------------------------------------
 * Expert service specific error messages
 *------------------------------------------------------------------------
 */
char *sys_experrlist[EEXPMAXERR-EEXPBASEOFF+2] =
  {"Error 0",
   "Expert service not active",
   "Illegal request",
   "Can't open the configuration file",
   "Request wasn't found in the configuration file",
   "Configuration file format error",
   "Can't launch execv()",
   "Can't change to working directory",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * DNS specific error messages
 *------------------------------------------------------------------------
 */
char *sys_dnserrlist[EDNSMAXERR-EDNSBASEOFF+2] =
  {"Error 0",
   "The specified host is unknown",
   "A temporary error occurred on an authoritative name server.  Try again later",
   "A non-recoverable name server error occurred",
   "The requested name is valid but does not have an IP address",
   "The requested name is valid but does not have an IP address",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * VDQM (Volume & Drive Queue Manager) specific error messages
 *------------------------------------------------------------------------
 */
char *sys_vqerrlist[EVQMAXERR-EVQBASEOFF+2] =
  {"Error 0",
   "Failed system call",
   "Internal DB inconsistency",
   "DB replication failed",
   "No volume request queued",
   "No free drive available",
   "Specified vol. req. not found",
   "Specified drv. req. not found",
   "Specified vol. req. already exists",
   "Unit not up",
   "Bad unit status request",
   "Incorrect vol.req or job ID",
   "Incorrect job ID",
   "Unit not assigned",
   "Attempt to mount with wrong VOLID",
   "Attempt to delete an assigned request",
   "Request for non-existing DGN",
   "Replication pipe is full",
   "Server is held",
   "End of query reached",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * RMC (Remote SCSI Media Changer server) specific error messages
 *------------------------------------------------------------------------
 */
char *sys_rmerrlist[ERMMAXERR-ERMBASEOFF+2] =
  {"Error 0",
   "Remote SCSI media changer server not active",
   "Remote SCSI media changer error",
   "Remote SCSI media changer unrec. error",
   "Remote SCSI media changer error (slow retry)",
   "Remote SCSI media changer error (fast retry)",
   "Remote SCSI media changer error (demount force)",
   "Remote SCSI media changer error (drive down)",
   "Remote SCSI media changer error (ops message)",
   "Remote SCSI media changer error (ops message + retry)",
   "Remote SCSI media changer error (ops message + wait)",
   "Remote SCSI media changer error (unload + demount)",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * Monitor specific error messages
 *------------------------------------------------------------------------
 */
char *sys_monerrlist[EMONMAXERR - EMONBASEOFF +2] =
  {"Error 0",
   "System Error",
   "Monitor Host not specified",
   "Monitor Port not specified",
   "Client port not specified",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * Security package errors
 *------------------------------------------------------------------------
 */
char *sys_secerrlist[ESECMAXERR-ESECBASEOFF+2] =
  {"Error 0",
   "System error",
   "Bad credentials",
   "Could not establish context",
   "Bad magic number",
   "Could not map username to uid/gid",
   "Could not map principal to username",
   "Could not load security mechanism",
   "Context not initialized",
   "Security protocol not supported",
   "Service name not set",
   "Service type not set",
   "Could not lookup security protocol",
   "Csec incompatibility",
   "Unexpected response from peer",
   "BAD ERROR NUMBER"
  };

/*
 *------------------------------------------------------------------------
 * End of package specific error messages
 *------------------------------------------------------------------------
 */

char *  sstrerror_r(n,buf,buflen)
     int     n;
     char    *buf;
     size_t  buflen;
{
  size_t msglen;
  char *tmpstr;
  if ( buf == NULL || buflen <= 0 ) return(NULL);
  memset(buf,'\0',buflen);
  tmpstr = NULL;

  if ((n>SEBASEOFF) && (n<=SEMAXERR)) {
    /*
     * COMMON error messages
     */
    tmpstr = sys_serrlist[n-SEBASEOFF];
  } else if ((n>ECBASEOFF) && (n<=ECMAXERR)) {
    /*
     * COPYTAPE specific error messages
     */
    tmpstr = sys_cerrlist[n-ECBASEOFF];
  } else if ((n>EDBBASEOFF) && (n<=EDBMAXERR)) {
    /*
     * DB specific error messages
     */
    tmpstr = sys_dberrlist[n-EDBBASEOFF];
  } else if ((n>EMSBASEOFF) && (n<=EMSMAXERR)) {
    /*
     * MSG specific error messages
     */
    tmpstr = sys_mserrlist[n-EMSBASEOFF];
  } else if ((n>ENSBASEOFF) && (n<=ENSMAXERR)) {
    /*
     * NS (Name server) specific error messages
     */
    tmpstr = sys_nserrlist[n-ENSBASEOFF];
  } else if ((n>ERFBASEOFF) && (n<=ERFMAXERR)) {
    /*
     * RFIO specific error messages
     */
    tmpstr = sys_rferrlist[n-ERFBASEOFF];
  } else if ((n>ERTBASEOFF) && (n<=ERTMAXERR)) {
    /*
     * RTCOPY specific error messages
     */
    tmpstr = sys_rterrlist[n-ERTBASEOFF];
  } else if ((n>ESTBASEOFF) && (n<=ESTMAXERR)) {
    /*
     * STAGE specific error messages
     */
    tmpstr = sys_sterrlist[n-ESTBASEOFF];
  } else if ((n>ESQBASEOFF) && (n<=ESQMAXERR)) {
    /*
     * SYSREQ specific error messages
     */
    tmpstr = sys_sqerrlist[n-ESQBASEOFF];
  } else if ((n>ETBASEOFF) && (n<=ETMAXERR)) {
    /*
     * TAPE specific error messages
     */
    tmpstr = sys_terrlist[n-ETBASEOFF];
  } else if ((n>EVMBASEOFF) && (n<=EVMMAXERR)) {
    /*
     * VMGR (Volume manager) specific error messages
     */
    tmpstr = sys_vaerrlist[n-EVMBASEOFF];
  } else if ((n>EUPBASEOFF) && (n<=EUPMAXERR)) {
    /*
     * UPV specific error messages
     */
    tmpstr = sys_uperrlist[n-EUPBASEOFF];
  } else if ((n>EEXPBASEOFF) && (n<=EEXPMAXERR)) {
    /*
     * Expert service specific error messages
     */
    tmpstr = sys_experrlist[n-EEXPBASEOFF];
  } else if ((n>EDNSBASEOFF) && (n<=EDNSMAXERR)) {
    /*
     * DNS specific error messages
     */
    tmpstr = sys_dnserrlist[n-EDNSBASEOFF];
  } else if ((n>EVQBASEOFF) && (n<=EVQMAXERR)) {
    /*
     * VDQM (Volume & Drive Queue Manager) specific error messages
     */
    tmpstr = sys_vqerrlist[n-EVQBASEOFF];
  } else if ((n>ERMBASEOFF) && (n<=ERMMAXERR)) {
    /*
     * RMC (Remote SCSI media changer server) specific error messages
     */
    tmpstr = sys_rmerrlist[n-ERMBASEOFF];
  } else if ((n>EMONBASEOFF) && (n<=EMONMAXERR)) {
    /*
     * Monitor specific error messages
     */
    tmpstr = sys_monerrlist[n-EMONBASEOFF];
  } else if ((n>ESECBASEOFF) && (n<=ESECMAXERR)) {
    /*
     * Security specific error messages
     */
    tmpstr = sys_secerrlist[n-ESECBASEOFF];
  } else if ((n>0)
#ifndef linux
             && (n<sys_nerr)
#endif
             ) {
    /*
     * SYSTEM error messages
     */
    tmpstr = (char *) strerror(n);
  }

  if ( tmpstr != NULL ) {
    msglen = strlen(tmpstr);
    if ( msglen >= buflen ) strncpy(buf,tmpstr,buflen-1);
    else strcpy(buf,tmpstr);
  } else {
    /*
     * Unknown error message
     */
    tmpstr = sys_serrlist[SEMAXERR+1-SEBASEOFF];
    msglen = strlen(tmpstr);
    if ( msglen+15 >= buflen ) strncpy(buf,tmpstr,buflen-1);
    else (void) sprintf(buf,"%s: %d", tmpstr,n);
  }
  return(buf);
}

void sperror(char    *msg)
{
  char buf[80];
  if (serrno)     {
    fprintf(stderr,"%s: %s\n",msg,sstrerror_r(serrno,buf,80));
  } else    {
    perror(msg);
  }
}

static int sstrerror_key = -1;

char *sstrerror(int n)
{
  char *buf = NULL;
  int buflen = 80;

  Cglobals_get(&sstrerror_key,(void **)&buf,buflen);
  return(sstrerror_r(n,buf,buflen));
}
