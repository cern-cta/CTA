/*
 * rfio_constants.h,v 1.13 2003/01/09 13:40:39 CERN IT-PDP/DM Olof Barring
 */

/*
 * Copyright (C) 2000-2002 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * rfio_constants.h  -   Remote File Access constants
 */

#ifndef _RFIO_CONSTANTS_H_INCLUDED_
#define _RFIO_CONSTANTS_H_INCLUDED_

/*
 * RFIO site-wide constants
 */
#define RFIO_NAME "rfio"
#define RFIO_PROTO "tcp"
#ifdef CSEC
#define RFIO_PORT 5501
#else
#define RFIO_PORT 5001
#endif

/*
 * RFIO options.
 */
#define RFIO_READOPT    1
#define RFIO_NETOPT     2
#define RFIO_NETRETRYOPT 3
#define RFIO_CONNECTOPT 4
#define RFIO_CONNECT_RETRY_COUNT_OPT	100
#define RFIO_CONNECT_RETRY_INT_OPT	200


#define RFIO_READBUF    1
#define RFIO_READAHEAD  2
#define RFIO_STREAM     16
#define RFIO_STREAM_DONE 32

#define RFIO_NONET      1
#define RFIO_NET        2
#define RFIO_RETRYIT    1
#define RFIO_NOTIME2RETRY 2
#define LLTM 1
#define LLM  2
#define RDLINKS         0
#define NORDLINKS       1
#define RFIO_NOLOCAL    0
#define RFIO_FORCELOCAL 1

/*
 * Internal (kernel) Constants
 */
#if defined(RFIO_KERNEL)

#if defined(_WIN32)
#include <Castor_limits.h>
#ifdef MAXHOSTNAMELEN
#undef MAXHOSTNAMELEN
#endif
#define         MAXHOSTNAMELEN  CA_MAXHOSTNAMELEN+1
#endif

#define RESHOSTNAMELEN 20
#define MAXMCON 20

#define LOCAL_IO        1       /* use local I/O for local file syntax  */

#define RFIO_MAGIC      0X0100       /* Remote File I/O magic number         */
#define B_RFIO_MAGIC    0X0200       /* Remote File I/O magic number         */

/*
 * netread()/netwrite() timeouts (seconds)
 */
#define RFIO_CTRL_TIMEOUT 20
#define RFIO_DATA_TIMEOUT 300

/*
 * Request message size.
 */
#define RQSTSIZE        3*(LONGSIZE+WORDSIZE)
#define RQSTSIZE64      3*WORDSIZE + 3*LONGSIZE + HYPERSIZE

/* FH to be in shift.conf, only default should be here */
/*efine IOBUFFERSIZE 128*1024 *//* Buffered read buffer                 */
#define DEFIOBUFSIZE 128*1024   /* Default IO buffer size               */

#define DEFRETRYCNT     38880   /* Default retry count 38880*60=1 week  */
#define DEFRETRYINT     60      /* Default retry interval               */
#define DEFCONNTIMEOUT  (2*60)  /* Default connect timeout limit =2mins.*/

#define _NFFILE 10              /* Maximum simultaneously open files    */
#define MAXFILENAMSIZE 1024     /* Maximum length of a file path name   */
#define MAXFILEMODSIZE  256     /* Maximum length of a file mode string */
#define MAXIOBUFSIZ 200*1024    /* Maximum network buffer size          */
#define MAXACCTSIZE     20      /* Maximum account name length          */
#define MAXFTNLUN       99      /* max number of Fortran logical units  */
#define MAXRFD         4096     /* max number of remote file descriptors*/
#define MAXCOMSIZ      1024     /* Maximum length of a user's command   */
#define MAXVMSTRING     256     /* Maximum length for VMS string        */
#if defined(HPSS)
#define RFIO_MAX_PORTS  10      /* Maximum number of network stripes    */
#endif /* HPSS */

#define DEFASNFNAM      ".rfassign"     /* default assign file name     */

#define FFTYPE_C        1       /* C File Format                        */
#define FFTYPE_F        2       /* Fortran File Format                  */


/* Fortran File Characteristics                                         */

#define FFFACC_S        0x0101  /* Fortran Sequential File              */
#define FFFACC_D        0x0102  /* Fortran Direct Access File           */
#define FFFFORM_D       0x0201  /* Fortran Formatted format             */
#define FFFFORM_U       0x0201  /* Fortran Unformatted format           */
#define FFFBLNK_N       0x0301  /* Fortran Blank to NULL                */
#define FFFBLNK_Z       0x0302  /* Fortran Blank to ZERO                */

/* Fortran READ method */

#define FFREAD_N        0x1001  /* Use native Fortran read              */
#define FFREAD_C        0x1002  /* Use subroutine Fortran read          */

/* Fortran OPEN options */

#define FFOOPT_0        0x0000  /* No options                           */
#define FFOOPT_A        0x0001  /* Open in append mode                  */
#define FFOOPT_E        0x0002  /* Erase on failure                     */
#define FFOOPT_T        0x0004  /* Truncate in direct format            */

/*
 * Request type
 */

#define RQST_ERRMSG     0x0100          /* request for remote error msg */
#define RQST_OPEN       0x2001          /* request for open()           */
#define RQST_READ       0x2002          /* request for read()           */
#define RQST_WRITE      0x2003          /* request for write()          */
#define RQST_CLOSE      0x2004          /* request for close()          */
#define RQST_READAHEAD  0x2005          /* request for read() ahead     */
#define RQST_FIRSTREAD  0x2006          /* first answer to readahead()  */
#define RQST_STAT       0x2007          /* request for stat()           */
#define RQST_FSTAT      0x2008          /* request for fstat()          */
#define RQST_LSEEK      0x2009          /* request for lseek()          */
#define RQST_FIRSTSEEK  0x200a          /* request for first preseek()  */
#define RQST_PRESEEK    0x200b          /* request for preseek()        */
#define RQST_LASTSEEK   0x200c          /* request for last preseek()   */
#define RQST_XYOPEN     0x3001          /* request for xyopen()         */
#define RQST_XYREAD     0x3002          /* request for xyread()         */
#define RQST_XYWRIT     0x3003          /* request for xywrit()         */
#define RQST_XYCLOS     0x3004          /* request for xyclos()         */
#define RQST_SYMLINK    0x3005          /* request for symlink()        */
#define RQST_STATFS     0x3006          /* request for statfs()         */
#define RQST_LSTAT      0x3007          /* request for lstat()          */
#define RQST_POPEN      0x3008          /* request for popen()          */
#define RQST_PCLOSE     0x3009          /* request for pclose()         */
#define RQST_FREAD      0x300a          /* request for fread()          */
#define RQST_FWRITE     0x300b          /* request for fwrite()         */
#define RQST_ACCESS     0x300c          /* request for access()         */
#define RQST_CHKCON     0x4001          /* request for connection check */
#define RQST_READLINK   0x4002          /* request for readlink()       */
#define RQST_MKDIR      0x4003          /* request for mkdir()          */
#define RQST_CHOWN      0x4004          /* request for chown()          */
#define RQST_RENAME     0x4005          /* request for rename()         */
#define RQST_LOCKF      0x4006          /* request for lockf()          */
#define RQST_MSTAT      0x4007          /* request for rfio_mstat()     */
#define RQST_END        0x4008          /* request for end of rfiod     */
#define RQST_MSTAT_SEC  0x4010          /* request for secure mstat()   */
#define RQST_STAT_SEC   0x4011          /* request for secure stat()    */
#define RQST_LSTAT_SEC  0x4012          /* request for secure lstat()   */
#define RQST_MSYMLINK   0x4013          /* request for msymlink()       */

#define REP_ERROR       0x5000
#define REP_EOF         0x5001
#define RQST_OPEN_V3    0x5003
#define RQST_CLOSE_V3   0x5004
#define RQST_READ_V3    0x5005
#define RQST_WRITE_V3   0x5006
#define RQST_LSEEK_V3   0x5007
#define RQST_FCHMOD     0x5008
#define RQST_FCHOWN     0x5009
#define RQST_OPENDIR    0x500a          /* request for opendir()        */
#define RQST_READDIR    0x500b          /* request for readdir()        */
#define RQST_CLOSEDIR   0x500c          /* request for closedir()       */
#define RQST_REWINDDIR  0x500d          /* request for rewinddir()      */
#define RQST_RMDIR      0x500e          /* request for rmdir()          */
#define RQST_CHMOD      0x500f          /* request for chmod()          */

#if defined(HPSS)
#define RQST_READLIST   0x6002
#define RQST_WRITELIST  0x6003
#define RQST_SETCOS     0x6004
#define RQST_READLIST64 0x6802
#define RQST_WRITLIST64 0x6803
#define RQST_SETCOS64   0x6804
#endif /* HPSS */

#define RQST_OPEN64     0x2801          /* request for open64()         */
#define RQST_READ64     0x2802          /* request for read64()         */
#define RQST_WRITE64    0x2803          /* request for write64()        */
#define RQST_CLOSE64    0x2804          /* request for close64()        */
#define RQST_READAHD64  0x2805          /* request for read64() ahead   */
#define RQST_FIRSTRD64  0x2806          /* first answer to read64ahead()*/
#define RQST_STAT64     0x2807          /* request for stat64()         */
#define RQST_FSTAT64    0x2808          /* request for fstat64()        */
#define RQST_LSEEK64    0x2809          /* request for lseek64()        */
#define RQST_PRESEEK64  0x280b          /* request for preseek64()      */
#define RQST_STATFS64   0x3806          /* request for statfs64()       */
#define RQST_LSTAT64    0x3807          /* request for lstat64()        */
#define RQST_FREAD64    0x380a          /* request for fread64()        */
#define RQST_FWRITE64   0x380b          /* request for fwrite64()       */
#define RQST_LOCKF64    0x3846          /* request for lockf()          */
#define RQST_MSTAT64    0x4807          /* request for rfio_mstat()     */
#define RQST_OPEN64_V3  0x5803          /* request for open64_v3()      */
#define RQST_CLOSE64_V3 0x5804          /* request for close64_v3()     */
#define RQST_READ64_V3  0x5805          /* request for read64_v3()      */
#define RQST_WRITE64_V3 0x5806          /* request for write64_v3()     */
#define RQST_LSEEK64_V3 0x5807          /* request for lseek64_v3()     */

/*
 * Support HSM types in client API
 */
#define RFIO_HSM_BASETYPE 0x0
#define RFIO_HSM_CNS      RFIO_HSM_BASETYPE+1

#endif /* RFIO_KERNEL */

#endif /* _RFIO_CONSTANTS_H_INCLUDED_ */
