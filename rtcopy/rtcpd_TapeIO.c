/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/* 
 * rtcpd_TapeIO.c - Tape I/O routines 
 */

#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <zlib.h>
#if !defined(_WIN32)
#include <sys/ioctl.h>
#include <sys/time.h>
#else /* _WIN32 */
#include <io.h>
#include <winsock2.h>
#endif /* _WIN32 */
#include <sys/types.h>
#if (defined(_AIX) && defined(_IBMR2)) || defined (SOLARIS) || \
( defined(__osf__) && defined(__alpha) )
#include <sys/stat.h>
#endif /* _AIX && _IBMR2  ||  SOLARIS  || ( __alpha__ && __osf ) */

#if ! defined(_AIX) && !defined(_WIN32)
#include <sys/mtio.h>
#endif  /* ! _AIX && !_WIN32*/

#include <osdep.h>
#include <net.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <serrno.h>
#include <Ctape_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include "tplogger_api.h"

extern int AbortFlag;

char *getconfent _PROTO((char *, char *, int));
int gettperror _PROTO((int, char *, char **));
static int read_sony _PROTO((int, char *, int, tape_list_t *, file_list_t *));
static int write_sony _PROTO((int, char *, int, tape_list_t *, file_list_t *));

#if defined(_AIX) && defined(_IBMR2)
static char driver_name[7];
char *dvrname;
#endif /* _AIX && _IBMR2 */

static char labelid[2][4] = {"EOF","EOV"} ;

static char cfgCksumAlgorithm[CA_MAXCKSUMNAMELEN+1];

/*
 * Error writing tape.
 */
static int twerror(int fd, tape_list_t *tape, file_list_t *file) {
  int errcat,j;
  int trec = 0;
  char *msgaddr, *p;
  char confparam[32];
  int severity, status;
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;

  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  severity = RTCP_OK;

  tapereq = &tape->tapereq;

  filereq = &file->filereq;
  trec = file->trec;
  status = errno;
  rtcp_log(LOG_DEBUG,"twerror(%d) called with errno=%d, serrno=%d\n",
           fd,errno,serrno);
  tl_rtcpd.tl_log( &tl_rtcpd, 11, 5, 
                   "func"   , TL_MSG_PARAM_STR, "twerror",
                   "Message", TL_MSG_PARAM_STR, "called",
                   "fd"     , TL_MSG_PARAM_INT, fd,
                   "errno"  , TL_MSG_PARAM_INT, errno,  
                   "serrno" , TL_MSG_PARAM_INT, serrno );
  errno = status;

  switch(errno) {
  case ENXIO:  /* Drive not operational */
    rtcpd_AppendClientMsg(NULL, file, RT140, "CPDSKTP");
    severity = RTCP_RESELECT_SERV | RTCP_SYERR;
    break;
  case EIO:    /* I/O error */
    msgaddr = NULL;
    errcat = gettperror (fd, filereq->tape_path, &msgaddr);
    if ( errcat <= 0 ) errcat = EIO;
    if ( msgaddr == NULL ) msgaddr = sstrerror(errcat);
    rtcpd_AppendClientMsg(NULL, file,RT126,"CPDSKTP", msgaddr, trec+1);
    if ( filereq != NULL ) status = errcat;
    switch (errcat) {
    case ETHWERR:       /* Device malfunctioning.    */
      /*
       *  Try another server
       */
      severity = RTCP_RESELECT_SERV | RTCP_SYERR;
      break;
    case ETUNREC:       /* Unrecoverable media error */
    case ETBLANK:       /* Blank tape                */
      /* 
       * should not happen on write...
       */
      severity = RTCP_FAILED | RTCP_USERR;
      break;
    case ETPARIT:       /* Parity error              */
      /*
       * Check if there is any configured error action
       * for this medium errors (like sending a mail to
       * operator or raising an alarm).
       */
      severity = RTCP_RESELECT_SERV | RTCP_USERR;
      sprintf(confparam,"%s_ERRACTION",tapereq->dgn);
      if ( (p = getconfent("RTCOPYD",confparam,0)) != NULL ) {
        j = atoi(p);
        severity = j;
      }
      break;
    case ETNOSNS:       /* No sense data available   */
      severity = RTCP_RESELECT_SERV | RTCP_UNERR;
      break;
    default :
      severity = RTCP_FAILED | RTCP_UNERR;
      break;
    }
    break;
#if defined(_AIX) && defined(_IBMR2)
  case EMEDIA:               
    /* 
     * media surface error on IBM 
     */
    severity = RTCP_RESELECT_SERV | RTCP_USERR;

    /*
     * Check if there is any configured error action
     * for this medium errors (like sending a mail to
     * operator or raising an alarm).
     */
    sprintf(confparam,"%s_ERRACTION",tapereq->dgn);
    if ( (p = getconfent("RTCOPYD",confparam,0)) != NULL ) {
      j = atoi(p);
      severity = j;
    }
    rtcpd_AppendClientMsg(NULL, file, RT125, "CPTPDSK", 
                          sstrerror(errno), trec+1);
    break;
#endif
  case EINVAL:
    rtcpd_AppendClientMsg(NULL, file, RT119, "CPDSKTP", trec+1);
    severity = RTCP_FAILED | RTCP_USERR;
    break;
#if defined(sun)
    /* 
     * Should not happen: EACCES only occurs when the driver 
     * gave a wrong information to tpmnt. 
     */
  case EACCES :
    rtcpd_AppendClientMsg(NULL, file, RT116, "CPDSKTP", 
                          sstrerror(errno), trec+1);
    severity = RTCP_RESELECT_SERV | RTCP_SYERR;
    break;
#endif /* sun */
  default:
    rtcpd_AppendClientMsg(NULL, file, RT116, "CPDSKTP", 
                          sstrerror(errno>0 ? errno : serrno), trec+1);
    severity = RTCP_FAILED | RTCP_UNERR;
    break;
  }

  if ( filereq != NULL ) {
    rtcpd_SetReqStatus(NULL,file,status,severity);
    if ( (severity & RTCP_NORETRY) != 0 ) {
      /* 
       * If configured error action says noretry we
       * reset max_cpretry so that the client won't retry
       * on another server
       */
      filereq->err.max_cpretry = 0;
    } else {
      if ( (severity & RTCP_LOCAL_RETRY) || 
           (severity & RTCP_RESELECT_SERV) )
        filereq->err.max_cpretry--;
    }
  }
  return(severity);
}

/*
 * Error reading tape.
 */
static int trerror(int fd, tape_list_t *tape, file_list_t *file) {
  int errcat, j;
  int trec = 0;
  char *msgaddr, *p;
  char confparam[32];
  int severity, status;
  int skiponerr = 0;
  int keepfile = 0;
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;

  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  severity = RTCP_OK;

  tapereq = &tape->tapereq;

  trec = file->trec;
  filereq = &file->filereq;
  status = errno;
  skiponerr = ((filereq->rtcp_err_action & SKIPBAD) != 0 ? 1 : 0);
  keepfile = ((filereq->rtcp_err_action & KEEPFILE) != 0 ? 1 : 0);

  switch(errno) {
  case EIO:
    msgaddr = NULL;
    errcat = gettperror (fd, filereq->tape_path, &msgaddr);
    if ( errcat <= 0 ) errcat = EIO;
    if ( msgaddr == NULL ) msgaddr = sstrerror(errcat);
    rtcpd_AppendClientMsg(NULL, file, RT125, "CPTPDSK", 
                          msgaddr, trec+1);

    if ( filereq != NULL ) status = errcat;
    if ( skiponerr == 0 || errcat != ETPARIT ) {
      switch ( errcat ) { 
      case ETPARIT:       /* Parity error              */
        if ( keepfile != 0 )
          severity = RTCP_RESELECT_SERV | RTCP_MNYPARY;
        else
          severity = RTCP_RESELECT_SERV | RTCP_USERR;

        /*
         * Check if there is any configured error action
         * for this medium errors (like sending a mail to
         * operator or raising an alarm).
         */
        sprintf(confparam,"%s_ERRACTION",tapereq->dgn);
        if ( (p = getconfent("RTCOPYD",confparam,0)) != NULL ) {
          j = atoi(p);
          severity = j;
        }
        break;
      case ETHWERR:       /* Device malfunctioning.    */
        severity = RTCP_RESELECT_SERV | RTCP_SYERR;
        break;
      case ETBLANK:       /* Blank tape                */
        severity = RTCP_FAILED | RTCP_USERR;
        break;
      case ETUNREC:       /* Unrecoverable media error */ 
        if ( (trec > 0)  && (keepfile != 0) )
          severity = RTCP_FAILED | RTCP_MNYPARY;
        else 
          severity = RTCP_FAILED | RTCP_USERR;
        break;  
      default:
        severity = RTCP_FAILED | RTCP_UNERR;
        break;
      }
    } else {
      /* 
       * Option to skip bad block is set and fits the error 
       */
      rtcpd_AppendClientMsg(NULL, file, RT211, "CPTPDSK");
      severity = RTCP_NEXTRECORD;
    }
    break ;
#if defined(_AIX) && defined(_IBMR2)
  case EMEDIA:
    rtcpd_AppendClientMsg(NULL, file, RT125, "CPTPDSK", 
                          sstrerror(errno), trec+1);
    if ( skiponerr ) {  /* -E skip */
      rtcpd_AppendClientMsg(NULL, file, RT211, "CPTPDSK");
      severity = RTCP_NEXTRECORD;
      break;
    }

    if ( keepfile != 0 )
      severity = RTCP_RESELECT_SERV | RTCP_MNYPARY;
    else
      severity = RTCP_RESELECT_SERV | RTCP_USERR;

    /*
     * Check if there is any configured error action
     * for this medium errors (like sending a mail to
     * operator or raising an alarm).
     */
    sprintf(confparam,"%s_ERRACTION",tapereq->dgn);
    if ( (p = getconfent("RTCOPYD",confparam,0)) != NULL ) {
      j = atoi(p);
      severity = j;
    }
    break;
#endif
  case ENOSPC:
    rtcpd_AppendClientMsg(NULL, file, RT137, "CPTPDSK", trec+1);
    severity = RTCP_FAILED | RTCP_USERR ;
  case ENXIO:             /* Drive not operational */
    rtcpd_AppendClientMsg(NULL , file, RT140, "CPTPDSK");
    severity = RTCP_RESELECT_SERV | RTCP_SYERR;
    break;
  case ENOMEM:
  case EINVAL:
    rtcpd_AppendClientMsg(NULL,file, RT103, "CPTPDSK", trec+1);
    if ( skiponerr ) {
      rtcpd_AppendClientMsg(NULL, file, RT211, "CPTPDSK");
      severity = RTCP_NEXTRECORD;
    } else {
      severity = RTCP_FAILED | RTCP_USERR;
    }
    break;
  default:
    rtcpd_AppendClientMsg(NULL,file, RT113, "CPTPDSK", 
                          sstrerror(errno), trec+1);
    severity = RTCP_FAILED | RTCP_UNERR;
    break;
  }

  if ( filereq != NULL ) {
    rtcpd_SetReqStatus(NULL,file,status,severity);
    if ( (severity & RTCP_NORETRY) != 0 ) {
      /* 
       * If configured error action says noretry we
       * reset max_cpretry so that the client won't retry
       * on another server
       */
      filereq->err.max_cpretry = 0;
    } else {
      if ( (severity & RTCP_LOCAL_RETRY) || 
           (severity & RTCP_RESELECT_SERV) )
        filereq->err.max_cpretry--;
    }
  }
  return(severity);
}

/*
 * Opening tape file.
 */
int topen(tape_list_t *tape, file_list_t *file) {
  int fd , rc; 
  int tmode;
  char *p;
#if defined(_WIN32)
  int binmode = O_BINARY;
#else /* _WIN32 */
  int binmode = 0;
#endif /* _WIN32 */
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;

  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  tapereq = &tape->tapereq;
  filereq = &file->filereq;

  /*
   * Initialize global information on current file. 
   */
  file->trec = 0 ;
  file->sonyraw = 0;
  file->negotiate = 0;
  file->eovflag = 0 ;

  if ( tapereq->mode == WRITE_ENABLE ) tmode = O_RDWR | binmode;
  else {
    tmode = O_RDONLY | binmode;
#if !defined(SOLARIS)
    if ( file->sonyraw > 0 ) tmode = O_RDWR;
#endif /* SOLARIS */
  }

  file->cksumRoutine = (unsigned long (*) _PROTO((unsigned long,
                                                  const char *,
                                                  unsigned int)))NULL;
  if ( *filereq->castorSegAttr.segmCksumAlgorithm != '\0' ) {
    if ( strncmp(filereq->castorSegAttr.segmCksumAlgorithm,
                 RTCP_CKSUM_ADLER32,
                 CA_MAXCKSUMNAMELEN) == 0 ) {
      file->cksumRoutine = (unsigned long (*) _PROTO((unsigned long,
                                                      const char *,
                                                      unsigned int)))adler32;
    } else if ( strncmp(filereq->castorSegAttr.segmCksumAlgorithm,
                        RTCP_CKSUM_CRC32,
                        CA_MAXCKSUMNAMELEN) == 0 ) {
      file->cksumRoutine = (unsigned long (*) _PROTO((unsigned long,
                                                      const char *,
                                                      unsigned int)))crc32;
    } else {
      log(LOG_ERR,"topen() client specifies unknown cksum algorithm: %s\n",
          filereq->castorSegAttr.segmCksumAlgorithm);
      tl_rtcpd.tl_log( &tl_rtcpd, 3, 3, 
                       "func"   , TL_MSG_PARAM_STR, "topen",
                       "Message", TL_MSG_PARAM_STR, "client specifies unknown cksum algorithm",
                       "fd"     , TL_MSG_PARAM_STR, filereq->castorSegAttr.segmCksumAlgorithm );
      *filereq->castorSegAttr.segmCksumAlgorithm = '\0';
    }
  }
  
  while ( file->cksumRoutine == (unsigned long (*) _PROTO((unsigned long, 
                                                           const char *,
                                                           unsigned int)))NULL ) {

    /*
     * Set configured checksum algorithm once for the whole request to
     * avoid /etc/castor/castor.conf lookups for every segment. We can
     * do this reliably with a static variable since the tape IO
     * processing is sequential.
     */
    if ( *cfgCksumAlgorithm == '\0' ) {
      if ( (p = getenv("RTCOPYD_CKSUMALG")) != NULL ||
           (p = getconfent("RTCOPYD","CKSUMALG",0)) != NULL ) {
        strncpy(cfgCksumAlgorithm,p,CA_MAXCKSUMNAMELEN);        
      } else {
        strncpy(
                cfgCksumAlgorithm,
                RTCP_CKSUM_DEFAULT,
                CA_MAXCKSUMNAMELEN
                );
      }
    }  
    
    if ( strncmp(cfgCksumAlgorithm,
                 RTCP_CKSUM_ADLER32,
                 CA_MAXCKSUMNAMELEN) == 0 ) {
      file->cksumRoutine = (unsigned long (*) _PROTO((
                                                      unsigned long, 
                                                      const char *,
                                                      unsigned int
                                                      )))adler32;
    } else if ( strncmp(cfgCksumAlgorithm,
                        RTCP_CKSUM_CRC32,
                        CA_MAXCKSUMNAMELEN) == 0 ) {
      file->cksumRoutine = (unsigned long (*) _PROTO((
                                                      unsigned long, 
                                                      const char *,
                                                      unsigned int
                                                      )))crc32;
    } else if ( strncmp(cfgCksumAlgorithm,"none",CA_MAXCKSUMNAMELEN) == 0 || 
                strncmp(cfgCksumAlgorithm,"NONE",CA_MAXCKSUMNAMELEN) == 0 ) {
      rtcp_log(
               LOG_INFO,
               "topen() Segment checksum is disabled by configuration\n"
               );
      tl_rtcpd.tl_log( &tl_rtcpd, 10, 2, 
                       "func"   , TL_MSG_PARAM_STR, "topen",
                       "Message", TL_MSG_PARAM_STR, "Segment checksum is disabled by configuration" );
      break;
    } else {
      rtcp_log(
               LOG_ERR,
               "topen() Unknown segment checksum algorithm configuration: %s. Use default %s\n",
               cfgCksumAlgorithm,
               RTCP_CKSUM_DEFAULT
               );
      tl_rtcpd.tl_log( &tl_rtcpd, 3, 4, 
                       "func"         , TL_MSG_PARAM_STR, "topen",
                       "Message"      , TL_MSG_PARAM_STR, "Unknown segment checksum algorithm configuration",
                       "Configuration", TL_MSG_PARAM_STR, cfgCksumAlgorithm,
                       "Use default"  , TL_MSG_PARAM_STR, RTCP_CKSUM_DEFAULT );      
      strncpy(
              cfgCksumAlgorithm,
              RTCP_CKSUM_DEFAULT,
              CA_MAXCKSUMNAMELEN
              );
    } 
  }
 
  if ( file->cksumRoutine != (unsigned long (*) _PROTO((unsigned long, 
                                                        const char *,
                                                        unsigned int)))NULL ) {
    if ( *filereq->castorSegAttr.segmCksumAlgorithm == '\0' ) {
      strncpy(
              filereq->castorSegAttr.segmCksumAlgorithm,
              cfgCksumAlgorithm,
              CA_MAXCKSUMNAMELEN
              );
    }
 
    filereq->castorSegAttr.segmCksum = file->cksumRoutine(0L,Z_NULL,0);
  }
    
  /*
   * Opening file.
   */
  rtcp_log(LOG_DEBUG,"topen() calling open(%s,%o)\n",
           filereq->tape_path,tmode);
  {
          char __tmode[32];
          sprintf( __tmode, "%o", tmode );
          tl_rtcpd.tl_log( &tl_rtcpd, 11, 4, 
                           "func"     , TL_MSG_PARAM_STR, "topen",
                           "Message"  , TL_MSG_PARAM_STR, "calling open",
                           "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                           "tmode"    , TL_MSG_PARAM_STR, __tmode );      
  }
  fd= open(filereq->tape_path,tmode);

  if ( fd == -1 ) {
    rtcpd_SetReqStatus(NULL,file,errno,RTCP_FAILED | RTCP_SYERR);
    if ( tapereq->mode == WRITE_ENABLE ) 
      rtcpd_AppendClientMsg(NULL,file, RT111, "CPDSKTP", 
                            sstrerror(errno));
    else
      rtcpd_AppendClientMsg(NULL,file, RT111, "CPTPDSK", 
                            sstrerror(errno));
    rtcp_log(LOG_DEBUG,"topen() open(%s,%o) returned rc=%d, errno=%d\n",
             filereq->tape_path,tmode,fd,errno);
    {
            char __tmode[32];
            sprintf( __tmode, "%o", tmode );
            tl_rtcpd.tl_log( &tl_rtcpd, 11, 6, 
                             "func"     , TL_MSG_PARAM_STR, "topen",
                             "Message"  , TL_MSG_PARAM_STR, "returned",
                             "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                             "tmode"    , TL_MSG_PARAM_STR, __tmode,
                             "rc"       , TL_MSG_PARAM_INT, fd,
                             "errno"    , TL_MSG_PARAM_INT, errno );      
    }
    return(-1);
  }
  rtcp_log(LOG_DEBUG,"topen() open(%s,%o) returned fd=%d\n",
           filereq->tape_path,tmode,fd);
  {
          char __tmode[32];
          sprintf( __tmode, "%o", tmode );
          tl_rtcpd.tl_log( &tl_rtcpd, 11, 5, 
                           "func"     , TL_MSG_PARAM_STR, "topen",
                           "Message"  , TL_MSG_PARAM_STR, "returned",
                           "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                           "tmode"    , TL_MSG_PARAM_STR, __tmode,
                           "rc"       , TL_MSG_PARAM_INT, fd );      
  }


#if defined(_AIX) && defined(_IBMR2)
  /*
   * This call is currently necessary because drive error reporting
   * on IBM is different depending on the drive type (see 
   * gettperror() in Ctape software). It is thread unsafe since it
   * depends on the dvrname global but this is OK here since we 
   * only have one mover process with one tapeIO thread per tape unit.
   */
  if (getdvrnam (filereq->tape_path, driver_name) < 0)
    strcpy (driver_name, "tape");
  dvrname = driver_name;
#endif

#if !defined(_WIN32) && !(defined(_AIX) && !defined(ADSTAR))
  rtcp_log(LOG_DEBUG,"topen() call clear_compression_stats(%d,%s,%s)\n",
           fd,filereq->tape_path,tapereq->devtype);
  tl_rtcpd.tl_log( &tl_rtcpd, 11, 5, 
                   "func"             , TL_MSG_PARAM_STR, "topen",
                   "Message"          , TL_MSG_PARAM_STR, "call clear_compression_stats",
                   "fd"               , TL_MSG_PARAM_INT, fd,
                   "Tape path"        , TL_MSG_PARAM_STR, filereq->tape_path,
                   "Tape req dev type", TL_MSG_PARAM_STR, tapereq->devtype );
  serrno = errno = 0;
  rc = clear_compression_stats(fd,filereq->tape_path,tapereq->devtype);
  rtcp_log(LOG_DEBUG,
           "topen() clear_compression_stats() returned rc=%d, errno=%d, serrno=%d\n",
           rc,errno,serrno);
  tl_rtcpd.tl_log( &tl_rtcpd, 11, 5, 
                   "func"   , TL_MSG_PARAM_STR, "topen",
                   "Message", TL_MSG_PARAM_STR, "clear_compression_stats returned",
                   "rc"     , TL_MSG_PARAM_INT, rc,
                   "errno"  , TL_MSG_PARAM_INT, errno,
                   "serrno" , TL_MSG_PARAM_INT, serrno );
#endif /* !_WIN32 && !(_AIX && !ADSTAR) */

  if (fd > 0) {

          tl_rtcpd.tl_log( &tl_rtcpd, 34, 4, 
                           "func"     , TL_MSG_PARAM_STR, "topen",
                           "Message"  , TL_MSG_PARAM_STR, "successful",
                           "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                           "fd"       , TL_MSG_PARAM_INT, fd );
  }
  /*
   * Returning file descriptor.
   */
  return(fd); 
}

/*
 * Closing tape file.
 */
int tclose(int fd, tape_list_t *tape, file_list_t *file) {
  int rc = 0;
  int comp_rc = 0;
  int save_serrno = 0;
  int save_errno = 0;
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;
  char *errstr = NULL;
#if !defined(_WIN32) && !(defined(_AIX) && !defined(ADSTAR))
  COMPRESSION_STATS compstats;
#endif /* !_WIN32 && !(_AIX && !ADSTAR) */


  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    (void) close(fd) ;
    return(-1);
  }
  tapereq = &tape->tapereq;
  filereq = &file->filereq;
  rtcp_log(LOG_DEBUG,"tclose(%d) called\n",fd);
  tl_rtcpd.tl_log( &tl_rtcpd, 11, 3, 
                   "func"   , TL_MSG_PARAM_STR, "tclose",
                   "Message", TL_MSG_PARAM_STR, "called",
                   "fd"     , TL_MSG_PARAM_INT, fd );

  if ( file->sonyraw == 0 ) {
    if ( tapereq->mode == WRITE_ENABLE && file->trec>0 ) {
      rtcp_log(
               LOG_DEBUG,
               "tclose(%d): write trailer label, tape path %s, flag %s, nb recs %d\n",
               fd,
               filereq->tape_path,
               labelid[file->eovflag],
               file->trec
               );
      tl_rtcpd.tl_log( &tl_rtcpd, 11, 6, 
                       "func"     , TL_MSG_PARAM_STR, "tclose",
                       "Message"  , TL_MSG_PARAM_STR, "write trailer label",
                       "fd"       , TL_MSG_PARAM_INT, fd,
                       "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                       "Flag"     , TL_MSG_PARAM_STR, labelid[file->eovflag],
                       "nb recs"  , TL_MSG_PARAM_INT, file->trec );
      errno = serrno = 0;
      if ( (rc = wrttrllbl(fd,filereq->tape_path,labelid[file->eovflag],
                           file->trec)) < 0 ) {
        save_errno = errno;
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"tclose(%d) wrttrlbl(%s): errno=%d, serrno=%d\n",
                 fd,filereq->tape_path,save_errno,save_serrno);
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 6, 
                         "func"     , TL_MSG_PARAM_STR, "tclose",
                         "Message"  , TL_MSG_PARAM_STR, "wrttrlbl",
                         "fd"       , TL_MSG_PARAM_INT, fd,
                         "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                         "errno"    , TL_MSG_PARAM_INT, save_errno,
                         "serrno"   , TL_MSG_PARAM_INT, save_serrno );
        rc = -1;
        errstr = rtcpd_CtapeErrMsg();
        rtcp_log(LOG_ERR,"tclose(%d) wrttrlbl(%s): errmsg=%s\n",
                 fd,filereq->tape_path,
                 (errstr != NULL ? errstr : "(no message)"));
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 5, 
                         "func"     , TL_MSG_PARAM_STR, "tclose",
                         "Message"  , TL_MSG_PARAM_STR, "wrttrlbl",
                         "fd"       , TL_MSG_PARAM_INT, fd,
                         "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                         "errmsg"   , TL_MSG_PARAM_STR, (errstr != NULL ? errstr : "(no message)") );
        if ( save_serrno == ENOSPC ) {
          rtcpd_AppendClientMsg(NULL,file,RT124,"CPDSKTP");
          rtcpd_SetReqStatus(NULL,file,save_serrno,
                             RTCP_FAILED | RTCP_ENDVOL);
        } else {
          if ( errstr != NULL && *errstr != '\0' ) {
            (void)rtcpd_AppendClientMsg(NULL, file, RT126, 
                                        "CPDSKTP",errstr,file->trec+1);
            if ( save_serrno > 0 )
              rtcpd_SetReqStatus(NULL,file,save_serrno,
                                 RTCP_FAILED | RTCP_SYERR);
          }
        } 
        rtcp_log(LOG_ERR,"tclose(%d) wrttrllbl(%s): %s\n",fd,
                 filereq->tape_path,sstrerror(save_serrno));
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 5, 
                         "func"     , TL_MSG_PARAM_STR, "tclose",
                         "Message"  , TL_MSG_PARAM_STR, "wrttrlbl",
                         "fd"       , TL_MSG_PARAM_INT, fd,
                         "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                         "err"      , TL_MSG_PARAM_STR, sstrerror(save_serrno) );
        serrno = save_serrno;
      }
    }
  }
#if !defined(_WIN32) && !(defined(_AIX) && !defined(ADSTAR))
  if ( (rc != -1) && (file->trec > 0) ) {
    serrno = 0;
    errno = 0;
    memset(&compstats,'\0',sizeof(compstats));
    comp_rc = get_compression_stats(fd,filereq->tape_path,
                                    tapereq->devtype,&compstats);
    if ( comp_rc == 0 ) {
      if ( tapereq->mode == WRITE_ENABLE ) {
        rtcp_log(LOG_DEBUG,"compression: from_host %d, to_tape %d\n",
                 compstats.from_host,compstats.to_tape);
        tl_rtcpd.tl_log( &tl_rtcpd, 11, 4, 
                         "func"     , TL_MSG_PARAM_STR, "tclose",
                         "Message"  , TL_MSG_PARAM_STR, "compression",
                         "from_host", TL_MSG_PARAM_INT, compstats.from_host,
                         "to_tape"  , TL_MSG_PARAM_INT, compstats.to_tape );
        filereq->host_bytes = ((u_signed64)compstats.from_host) * 1024;
        filereq->bytes_out = ((u_signed64)compstats.to_tape) * 1024;
        if ( ((filereq->bytes_out == 0) && (filereq->host_bytes != 0)) || (filereq->bytes_out > filereq->host_bytes)) {
           filereq->bytes_out = filereq->host_bytes;
        }
  
      } else {
        rtcp_log(LOG_DEBUG,"compression: to_host %d, from_tape %d\n",
                 compstats.to_host,compstats.from_tape);
        tl_rtcpd.tl_log( &tl_rtcpd, 11, 4, 
                         "func"     , TL_MSG_PARAM_STR, "tclose",
                         "Message"  , TL_MSG_PARAM_STR, "compression",
                         "to_host"  , TL_MSG_PARAM_INT, compstats.to_host,
                         "from_tape", TL_MSG_PARAM_INT, compstats.from_tape );
        filereq->host_bytes = ((u_signed64)compstats.to_host) *1024;
        filereq->bytes_in = ((u_signed64)compstats.from_tape) * 1024;
      }
    } else {
      serrno = (serrno != 0 ? serrno : errno);
      if ( serrno != SEOPNOTSUP ) {
        rtcp_log(
                 LOG_ERR,
                 "get_compression_stats() failed, rc = %d: %s\n",
                 comp_rc,sstrerror(serrno)
                 );
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 4, 
                         "func"   , TL_MSG_PARAM_STR, "tclose",
                         "Message", TL_MSG_PARAM_STR, "compression_stats() failed",
                         "rc"     , TL_MSG_PARAM_INT, comp_rc,
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(serrno) );
      }
      if ( tapereq->mode == WRITE_ENABLE ) {
        filereq->host_bytes = filereq->bytes_out = filereq->bytes_in;
      } else {
        filereq->host_bytes = filereq->bytes_in = filereq->bytes_out;
      }
    }
  }
#endif /* !_WIN32 && !(_AIX && !ADSTAR) */
  (void) close(fd) ; 
  filereq->TEndTransferTape = (int)time(NULL);
  if ( file->cksumRoutine != (unsigned long (*) _PROTO((unsigned long, 
                                                        const char *,
                                                        unsigned int)))NULL ) {
    rtcp_log(LOG_INFO,"segment checksum (%s): %lu, 0x%x\n",
             filereq->castorSegAttr.segmCksumAlgorithm,
             filereq->castorSegAttr.segmCksum,
             filereq->castorSegAttr.segmCksum);
    {
            char __segmCksum_lu[32], __segmCksum_x[32];
            sprintf( __segmCksum_lu, "%lu", filereq->castorSegAttr.segmCksum );
            sprintf( __segmCksum_x, "0x%lx", filereq->castorSegAttr.segmCksum );
            tl_rtcpd.tl_log( &tl_rtcpd, 10, 5, 
                             "func"          , TL_MSG_PARAM_STR, "tclose",
                             "Message"       , TL_MSG_PARAM_STR, "segment checksum",
                             "Algorithm"     , TL_MSG_PARAM_STR, filereq->castorSegAttr.segmCksumAlgorithm,
                             "Checksum (dec)", TL_MSG_PARAM_STR, __segmCksum_lu, 
                             "Checksum (hex)", TL_MSG_PARAM_STR, __segmCksum_x );
    }
  }

  if (0 == rc) {

          tl_rtcpd.tl_log( &tl_rtcpd, 35, 4, 
                           "func"     , TL_MSG_PARAM_STR, "tclose",
                           "Message"  , TL_MSG_PARAM_STR, "successful",
                           "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                           "fd"       , TL_MSG_PARAM_INT, fd );
  }

  return(rc);
}

/*
 * Closing tape file on error.
 */
int tcloserr(int fd, tape_list_t *tape, file_list_t *file) {
  int rc = 0;
  int save_errno, save_serrno;
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;

  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    (void) close(fd) ;
    return(-1);
  }
  filereq = &file->filereq;
  tapereq = &tape->tapereq;

  rtcp_log(LOG_DEBUG,"tcloserr(%d) called\n",fd);
  tl_rtcpd.tl_log( &tl_rtcpd, 11, 3, 
                   "func"   , TL_MSG_PARAM_STR, "tcloserr",
                   "Message", TL_MSG_PARAM_STR, "called",
                   "fd"     , TL_MSG_PARAM_INT, fd );

  if ( tapereq->mode == WRITE_ENABLE ) {
    if ( file->trec > 0 ) {
      rtcp_log(
               LOG_INFO,
               "tcloserr(%d) delete tape file, tape path %s\n",
               fd,
               filereq->tape_path
               );
      tl_rtcpd.tl_log( &tl_rtcpd, 10, 4, 
                       "func"     , TL_MSG_PARAM_STR, "tcloserr",
                       "Message"  , TL_MSG_PARAM_STR, "delete tape file",
                       "fd"       , TL_MSG_PARAM_INT, fd,
                       "Tape path", TL_MSG_PARAM_STR, filereq->tape_path );
    }
    errno = serrno = 0;
    if ( file->trec>0 && (rc = deltpfil(fd,filereq->tape_path)) < 0 ) {
      save_errno = errno;
      save_serrno = serrno;
      if ( AbortFlag == 0 ) 
        rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED | RTCP_SYERR);
      rtcpd_AppendClientMsg(NULL,file, RT141, "CPDSKTP");
      rtcp_log(LOG_ERR,"deltpfil(%d,%s) failed with errno=%d, serrno=%d\n",
               fd,filereq->tape_path,save_errno,save_serrno);
      tl_rtcpd.tl_log( &tl_rtcpd, 3, 6, 
                       "func"     , TL_MSG_PARAM_STR, "tcloserr",
                       "Message"  , TL_MSG_PARAM_STR, "deltpfil failed",
                       "fd"       , TL_MSG_PARAM_INT, fd,
                       "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                       "errno"    , TL_MSG_PARAM_INT, errno,     
                       "serrno"   , TL_MSG_PARAM_INT, serrno );
    }
  }
  (void) close(fd) ;
  filereq->TEndTransferTape = (int)time(NULL);
  return(0);
}

/*
 * Writing tape block.
 */
int twrite(int fd,char *ptr,int len, 
           tape_list_t *tape, file_list_t *file) {
  int     rc, save_serrno, save_errno;
  char *errstr;
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;

  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    (void) close(fd) ;
    return(-1);
  }
  tapereq = &tape->tapereq;
  filereq = &file->filereq;

  if ( file->sonyraw == 0 ) {
    /*
     * Writing header labels.
     */
    if ( file->trec == 0 ) {
      filereq->TStartTransferTape = (int)time(NULL);
      rtcp_log(LOG_DEBUG,"twrite(%d): write header label, tape_path=%s\n",
               fd,filereq->tape_path);
      tl_rtcpd.tl_log( &tl_rtcpd, 11, 4, 
                       "func"     , TL_MSG_PARAM_STR, "twrite",
                       "Message"  , TL_MSG_PARAM_STR, "write header label",
                       "fd"       , TL_MSG_PARAM_INT, fd,
                       "Tape path", TL_MSG_PARAM_STR, filereq->tape_path );
      errno = serrno = 0;
      if ( (rc = wrthdrlbl(fd,filereq->tape_path)) < 0 ) {
        save_errno = errno;
        save_serrno = serrno;
        if ( save_serrno == ENOSPC ) {
          rtcpd_AppendClientMsg(NULL,file,RT124,"CPDSKTP");
          rtcpd_SetReqStatus(NULL,file,save_serrno,
                             RTCP_FAILED | RTCP_ENDVOL);
        } else {
          errstr = rtcpd_CtapeErrMsg();
          if ( errstr != NULL && *errstr != '\0' ) {
            (void)rtcpd_AppendClientMsg(NULL, file, RT126,
                                        "CPDSKTP", errstr, file->trec+1);
            if ( save_serrno > 0 )
              rtcpd_SetReqStatus(NULL,file,save_serrno,
                                 RTCP_FAILED | RTCP_SYERR);
            rtcp_log(LOG_ERR,"twrite(%d) wrthdrlbl(%s): %s\n",
                     fd,filereq->tape_path,errstr);
            tl_rtcpd.tl_log( &tl_rtcpd, 3, 5, 
                             "func"     , TL_MSG_PARAM_STR, "twrite",
                             "Message"  , TL_MSG_PARAM_STR, "wrthdrlbl",
                             "fd"       , TL_MSG_PARAM_INT, fd,
                             "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                             "error"    , TL_MSG_PARAM_STR, errstr );
          }
        }
        rtcp_log(
                 LOG_ERR,
                 "twrite(%d) wrthdrlbl(%s): %s\n",
                 fd,
                 filereq->tape_path,
                 sstrerror(save_serrno)
                 );
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 5, 
                         "func"     , TL_MSG_PARAM_STR, "twrite",
                         "Message"  , TL_MSG_PARAM_STR, "wrthdrlbl",
                         "fd"       , TL_MSG_PARAM_INT, fd,
                         "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                         "error"    , TL_MSG_PARAM_STR, sstrerror(save_serrno) );
        rtcp_log(
                 LOG_DEBUG,
                 "twrite(%d): wrthdrlbl(%d,%s) returns %d, errno=%d, serrno=%d\n",
                 fd,
                 fd,
                 filereq->tape_path,
                 rc,
                 save_errno,
                 save_serrno
                 );
        tl_rtcpd.tl_log( &tl_rtcpd, 11, 7, 
                         "func"     , TL_MSG_PARAM_STR, "twrite",
                         "Message"  , TL_MSG_PARAM_STR, "wrthdrlbl",
                         "fd"       , TL_MSG_PARAM_INT, fd,
                         "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                         "returns"  , TL_MSG_PARAM_INT, rc,
                         "errno"    , TL_MSG_PARAM_INT, save_errno,
                         "serrno"   , TL_MSG_PARAM_INT, save_serrno );
        
        serrno = save_serrno;
        return(-1);
      }
    }
    
    /*
     * Writing data.
     * trec is incremented to know how many BLOCKs
     * have been written (needed for trailing labels).
     */
    file->trec ++ ;
    rc= write(fd,ptr,len) ;
    /*
     * There is an error.
     */
    if ( rc == -1 ) {
#if defined(_IBMR2)
      if ( errno == ENOSPC || errno == ENXIO)
#else
        if ( errno == ENOSPC )
#endif
          file->eovflag= 1; /* tape volume overflow */
        else
          twerror(fd,tape,file) ;
      rtcp_log(
               LOG_DEBUG,
               "twrite(%d): write(%d,0x%x,%d) returns %d, errno=%d, serrno=%d\n",
               fd,
               fd,
               ptr,
               len,
               rc,
               errno,
               serrno
               );
      {
              char __ptr[32];
              sprintf( __ptr, "0x%lx", (unsigned long)ptr );
              tl_rtcpd.tl_log( &tl_rtcpd, 11, 8, 
                               "func"   , TL_MSG_PARAM_STR, "twrite",
                               "Message", TL_MSG_PARAM_STR, "write",
                               "fd"     , TL_MSG_PARAM_INT, fd,
                               "ptr"    , TL_MSG_PARAM_STR, __ptr,
                               "len"    , TL_MSG_PARAM_INT, len,                               
                               "returns", TL_MSG_PARAM_INT, rc,
                               "errno"  , TL_MSG_PARAM_INT, errno,
                               "serrno" , TL_MSG_PARAM_INT, serrno );
      }
    }

#if defined(sun) || defined(sgi)
    if ( rc == 0 ) {
      struct mtget mt_info ;

      if ( ioctl(fd,MTIOCGET,&mt_info) == -1 ) {
        rtcpd_SetReqStatus(NULL,file,errno,RTCP_FAILED | RTCP_SYERR);
        rtcpd_AppendClientMsg(NULL,file, RT109, "CPDSKTP", 
                              sstrerror(errno));
        return(-1);
      }
#if defined(sun)
      if ( mt_info.mt_erreg == 19 ) /* SC_EOT */
#else
      if ( mt_info.mt_erreg == 0 )
#endif  /* sun */
      {
        file->eovflag= 1; /* tape volume overflow */
      } else {
        rtcpd_AppendClientMsg(NULL,file, RT117, "CPDSKTP", 
                              mt_info.mt_erreg, file->trec+1);
#if defined(sun)
        rtcpd_SetReqStatus(NULL,file,EINVAL,
                           RTCP_RESELECT_SERV | RTCP_UNERR);
        filereq->err.max_cpretry--;
#else
        rtcpd_SetReqStatus(NULL,file,EINVAL,
                           RTCP_FAILED | RTCP_UNERR);
#endif  /* sun */
        return(-1);
      }
    }
    
#endif  /* sun || sgi */
    if ( file->eovflag ) {
      file->trec-- ;
      return(0);
    }
  } else {
    if ( file->trec == 0 ) filereq->TStartTransferTape = (int)time(NULL);
    file->trec ++ ;
    rc = write_sony(fd, ptr, len, tape, file);
  }

  if ( (rc > 0) && (file->cksumRoutine != 
                    (unsigned long (*) _PROTO((unsigned long, 
                                               const char *,
                                               unsigned int)))NULL) ) {
    filereq->castorSegAttr.segmCksum = 
      file->cksumRoutine(
                         filereq->castorSegAttr.segmCksum,
                         ptr,
                         (unsigned int)rc
                         );
  }
    
  return(rc);
}

/*
 * Reading tape block.
 */
int tread(int fd,char *ptr,int len,
          tape_list_t *tape, file_list_t *file) {
  int     rc, status, save_serrno;
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;

  if ( tape == NULL || file == NULL ) {
    serrno = EINVAL;
    (void) close(fd) ;
    return(-1);
  }
  tapereq = &tape->tapereq;
  filereq = &file->filereq;

  if ( file->trec == 0 ) filereq->TStartTransferTape = (int)time(NULL);

  /*
   * Reading block.
   */
  if ( file->sonyraw == 0 ) {
    if ( (rc= read(fd,ptr,len)) == -1 )  {
      /* 
       * trerror returns a code when -E option is set to 
       * skip when an error on the tape is encountered
       */
      status = trerror(fd,tape,file) ;
      if ( (status & RTCP_NEXTRECORD) ) {
        file->trec ++ ;
        return(0) ;
      }
    }

    /*
     * Count the number of blocks
     */
    file->trec ++ ;

    /*
     * Blank tape may have been encountered.
     */
#if defined(sun)
    if ( rc == 0 ) {
      char *msgaddr = NULL;

      if (gettperror (fd, filereq->tape_path, &msgaddr) == ETBLANK) {
        rtcpd_AppendClientMsg(NULL, file, RT118,"CPTPDSK",
                              file->trec);
        if (file->trec && filereq->rtcp_err_action == KEEPFILE ) {
          rtcpd_SetReqStatus(NULL,file,ETBLANK,    
                             RTCP_RESELECT_SERV | RTCP_OK);
          filereq->err.max_cpretry--;
        } else
          rtcpd_SetReqStatus(NULL,file,ETBLANK,    
                             RTCP_FAILED | RTCP_USERR);

        rtcp_log(LOG_ERR,RT118,"CPTPDSK",file->trec);
        tl_rtcpd.tl_log( &tl_rtcpd, 118, 3, 
                         "func"   , TL_MSG_PARAM_STR, "tread",
                         "Message", TL_MSG_PARAM_STR, "CPTPDSK",
                         "Block #", TL_MSG_PARAM_INT, file->trec );        
      }
      return(-1);
    }
#endif  /* sun  */
    /*
     * Returning number of bytes read.
     */
    if ( rc == 0 ) {
      rtcp_log(
               LOG_DEBUG,
               "tread(%d,%s) check EOF/EOV\n",
               fd,
               filereq->tape_path
               );
      tl_rtcpd.tl_log( &tl_rtcpd, 11, 4, 
                       "func"     , TL_MSG_PARAM_STR, "tread",
                       "Message"  , TL_MSG_PARAM_STR, "check EOF/EOV",
                       "fd"       , TL_MSG_PARAM_INT, fd,
                       "Tape path", TL_MSG_PARAM_STR, filereq->tape_path );        

      if ((rc = checkeofeov(fd, filereq->tape_path)) < 0) {
        save_serrno = serrno;
        rtcp_log(
                 LOG_DEBUG,
                 "tread(%d,%s) checkeofeov() returns %d, serrno=%d\n",
                 fd,
                 filereq->tape_path,
                 rc,
                 save_serrno
                 );
        tl_rtcpd.tl_log( &tl_rtcpd, 11, 6, 
                         "func"       , TL_MSG_PARAM_STR, "tread",
                         "Message"    , TL_MSG_PARAM_STR, "checkeofeov()",
                         "fd"         , TL_MSG_PARAM_INT, fd,
                         "Tape path"  , TL_MSG_PARAM_STR, filereq->tape_path,
                         "returns"    , TL_MSG_PARAM_INT, rc, 
                         "save_serrno", TL_MSG_PARAM_INT, save_serrno );        

        if (save_serrno == ETLBL) {
          rtcpd_AppendClientMsg(
                                NULL, 
                                file, 
                                RT128,
                                "CPTPDSK"
                                );
          rtcpd_SetReqStatus(
                             NULL,
                             file,
                             save_serrno,
                             RTCP_FAILED | RTCP_USERR
                             );
          rtcp_log(
                   LOG_ERR,
                   RT128,
                   "CPTPDSK"
                   );
          tl_rtcpd.tl_log( &tl_rtcpd, 128, 2, 
                           "func"   , TL_MSG_PARAM_STR, "tread",
                           "Message", TL_MSG_PARAM_STR, "CPTPDSK" );
        } else
          rtcpd_SetReqStatus(
                             NULL,
                             file,
                             save_serrno,
                             RTCP_FAILED | RTCP_UNERR
                             );
        return(-1);
      }
      rtcp_log(
               LOG_DEBUG,
               "tread(%d,%s) checkeofeov() returns %d, serrno=%d\n",
               fd,
               filereq->tape_path,
               rc,
               serrno
               );
      tl_rtcpd.tl_log( &tl_rtcpd, 11, 6, 
                       "func"     , TL_MSG_PARAM_STR, "tread",
                       "Message"  , TL_MSG_PARAM_STR, "checkeofeov()",
                       "fd"       , TL_MSG_PARAM_INT, fd,
                       "Tape path", TL_MSG_PARAM_STR, filereq->tape_path,
                       "returns"  , TL_MSG_PARAM_INT, rc, 
                       "serrno"   , TL_MSG_PARAM_INT, serrno );        

      if ( rc == 0 ) return(0);   /* end of file has been reached */
      file->eovflag= 1;           /* tape volume overflow */
      return(0);
    }
  } else {
    file->trec ++ ;
    rc = read_sony(fd, ptr, len, tape, file);
  }
  if ( (rc > 0) && (file->cksumRoutine != 
                    (unsigned long (*) _PROTO((
                                               unsigned long,
                                               const char *,
                                               unsigned int)))NULL) ) {
    filereq->castorSegAttr.segmCksum =
      file->cksumRoutine(
                         filereq->castorSegAttr.segmCksum,
                         ptr,
                         (unsigned int)rc
                         );
  }
  return(rc);
}

/*  tpio_sony - sends scsi commands to support RAW mode on SONY DIR1000
    using a DFC-1500/1700 controller */

static int read_sony(int fd, char *buf, int len, 
                     tape_list_t *tape, file_list_t *file) {
  return(-1);
}
        
static int write_sony(int fd, char *buf, int len,
                      tape_list_t *tape,
                      file_list_t *file ) {
  return(-1);
}
