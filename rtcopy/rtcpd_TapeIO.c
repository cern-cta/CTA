/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_TapeIO.c,v $ $Revision: 1.7 $ $Date: 2000/01/09 09:53:49 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/* 
 * rtcpd_TapeIO.c - Tape I/O routines 
 */

#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#if !defined(_WIN32)
#include <sys/ioctl.h>
#include <sys/time.h>
#else /* _WIN32 */
#include <io.h>
#include <time.h>
#include <winsock2.h>
#endif /* _WIN32 */
#include <sys/types.h>
#if (defined(_AIX) && defined(_IBMR2)) || defined (SOLARIS) || ( defined(__osf__) && defined(__alpha) )
#include <sys/stat.h>
#endif /* _AIX && _IBMR2  ||  SOLARIS  || ( __alpha__ && __osf ) */

#if ! defined(_AIX) && !defined(_WIN32)
#include <sys/mtio.h>
#endif  /* ! _AIX && !_WIN32*/

#if SONYRAW
#include <scsictl.h>
#endif

#include <osdep.h>
#include <net.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <serrno.h>
#include <Ctape_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>

#if defined(__STDC__)
char *getconfent(char *, char *, int);
int gettperror(int, char **);
static int read_sony(int, char *, int);
static int write_sony(int, char *, int);
#else /* __STDC__ */
char *getconfent();
int gettperror();
static int read_sony();
static int write_sony();
#endif /* __STDC__ */

char *devtype;            /* Extern needed by Ctape */

#if defined(_AIX) && defined(_IBMR2)
static char driver_name[7];
char *dvrname;
#endif /* _AIX && _IBMR2 */

static char labelid[2][4] = {"EOF","EOV"} ;

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

    severity = RTCP_OK;

    if ( tape != NULL ) tapereq = &tape->tapereq;

    if ( file != NULL ) {
        filereq = &file->filereq;
        trec = file->trec;
        status = errno;
    }

    switch(errno) {
        case ENXIO:  /* Drive not operational */
            rtcpd_AppendClientMsg(NULL, file, RT140, "CPDSKTP");
            severity = RTCP_RESELECT_SERV | RTCP_SYERR;
            break;
        case EIO:    /* I/O error */
            errcat = gettperror (fd, &msgaddr);
            rtcpd_AppendClientMsg(NULL, file,RT126,"CPDSKTP", msgaddr, trec+1);
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
                    sprintf(confparam,"%s_ERRACTION",tapereq->dgn);
                    if ( (p = getconfent("RTCOPYD",confparam,0)) != NULL ) {
                        j = atoi(p);
                        severity = j;
                    }
                case ETNOSNS:       /* No sense data available   */
                    if ( !(severity & RTCP_NORETRY) ) {
                        severity = RTCP_RESELECT_SERV;
                    }
                default :
                    severity = severity | RTCP_UNERR;
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
             * Should not happen :EACCES only occurs when the driver 
             * gave a wrong information to tpmnt. Then exit with SYRETRYD
             */
        case EACCES :
            rtcpd_AppendClientMsg(NULL, file, RT116, "CPDSKTP", 
                sstrerror(errno), trec+1);
            severity = RTCP_RESELECT_SERV | RTCP_SYERR;
            break;
#endif /* sun */
        default:
            rtcpd_AppendClientMsg(NULL, file, RT116, "CPDSKTP", 
                sstrerror(errno), trec+1);
            severity = RTCP_FAILED | RTCP_UNERR;
            break;
    }

    if ( filereq != NULL ) {
        rtcpd_SetReqStatus(NULL,file,status,severity);
        if ( (severity & RTCP_NORETRY) ) {
            /* 
             * If configured error action says noretry we
             * reset max_tpretry so that the client won't retry
             * on another server
             */
            filereq->err.max_cpretry = -1;
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

    severity = RTCP_OK;

    if ( tape != NULL ) {
        tapereq = &tape->tapereq;
    }

    if ( file != NULL ) {
        trec = file->trec;
        filereq = &file->filereq;
        status = errno;
        skiponerr = (filereq->rtcp_err_action == SKIPBAD ? 1 : 0);
        keepfile = (filereq->rtcp_err_action == KEEPFILE ? 1 : 0);
    }

    switch(errno) {
        case EIO:
            errcat = gettperror (fd, &msgaddr);
            rtcpd_AppendClientMsg(NULL, file, RT125, "CPTPDSK", 
                                  msgaddr, trec+1);

            if ( filereq != NULL ) status = errcat;
            if ( skiponerr == 0 || errcat != ETPARIT ) {
                switch ( errcat ) { 
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
                case ETHWERR:       /* Device malfunctioning.    */
                    severity = RTCP_RESELECT_SERV | RTCP_SYERR;
                    break;
                case ETBLANK:       /* Blank tape                */
                    severity = RTCP_FAILED | RTCP_USERR;
                    break;
                case ETUNREC:       /* Unrecoverable media error */ 
                    if (trec && keepfile)
                        severity = RTCP_RESELECT_SERV | RTCP_OK;
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
        if ( (severity & RTCP_NORETRY) ) {
            /* 
             * If configured error action says noretry we
             * reset max_tpretry so that the client won't retry
             * on another server
             */
            filereq->err.max_cpretry = -1;
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
    int fd ; 
    int tmode;
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
#if defined(SONYRAW)
    if ( strcmp(tapereq->density,"RAW") ) file->sonyraw++;
#endif /* SONYRAW */
    file->negotiate = 0;
    file->eovflag = 0 ;
    /*
     * Needed by tperror() in Ctape
     */
    devtype = tapereq->devtype;

    if ( tapereq->mode == WRITE_ENABLE ) tmode = O_RDWR | binmode;
    else {
        tmode = O_RDONLY | binmode;
#if !defined(SOLARIS)
        if ( file->sonyraw > 0 ) tmode = O_RDWR;
#endif /* SOLARIS */
    }
    /*
     * Opening file.
     */
    fd= open(filereq->tape_path,tmode);

    if ( fd == -1 ) {
        rtcpd_SetReqStatus(NULL,file,errno,RTCP_FAILED | RTCP_SYERR);
		if ( tapereq->mode == WRITE_ENABLE ) 
			rtcpd_AppendClientMsg(NULL,file, RT111, "CPDSKTP", 
                                  sstrerror(errno));
		else
			rtcpd_AppendClientMsg(NULL,file, RT111, "CPTPDSK", 
                                  sstrerror(errno));
        return(-1);
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
    clear_compression_stats(fd,filereq->tape_path,tapereq->devtype);
#endif /* !_WIN32 && !(_AIX && !ADSTAR) */
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
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
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

    if ( file->sonyraw == 0 ) {
        if ( tapereq->mode == WRITE_ENABLE && file->trec>0 ) {
            rtcp_log(LOG_DEBUG,"tclose(%d): write trailer label, tape path %s, flag %s, nb recs %d\n",
                fd,filereq->tape_path,labelid[file->eovflag],file->trec);
            if ( wrttrllbl(fd,filereq->tape_path,labelid[file->eovflag],
                 file->trec) < 0 ) {
                rc = -1;
#if defined(_IBMR2)
                if ( (errno == ENOSPC) || (errno == ENXIO) ) 
                    serrno = ENOSPC;
#else /* _IBMR2 */
                if ( errno == ENOSPC ) 
                    serrno = ENOSPC;
#endif /* _IBMR2 */
                else twerror(fd,tape,file);
                rtcp_log(LOG_ERR,"tclose(%d) wrttrllbl(): %s\n",
                    sstrerror(serrno));
            }
        }
    }
#if !defined(_WIN32) && !(defined(_AIX) && !defined(ADSTAR))
    if ( (rc != -1) && (file->trec > 0) ) {
        serrno = 0;
        errno = 0;
        comp_rc = get_compression_stats(fd,filereq->tape_path,
                                        tapereq->devtype,&compstats);
        if ( comp_rc == 0 ) {
            if ( tapereq->mode == WRITE_ENABLE ) {
                rtcp_log(LOG_DEBUG,"compression: from_host %d, to_tape %d\n",
                    compstats.from_host,compstats.to_tape);
                filereq->bytes_from_host = ((u_signed64)compstats.from_host) * 1024;
                filereq->bytes_out = ((u_signed64)compstats.to_tape) * 1024;
            } else {
                rtcp_log(LOG_ERR,"compression: to_host %d, from_tape %d\n",
                    compstats.to_host,compstats.from_tape);
                if ( compstats.to_host != 0 &&
                     compstats.to_host != 
                     (unsigned long)(file->tapebytes_sofar / 1024) ) {
                    rtcp_log(LOG_ERR,"tclose() inconsistent nb bytes to host %ld<->%ld\n",
                        compstats.to_host,
                        (unsigned long)(file->tapebytes_sofar / 1024));
                }
                filereq->bytes_in = ((u_signed64)compstats.from_tape) * 1024;
            }
        } else {
            serrno = (serrno != 0 ? serrno : errno);
            rtcp_log(LOG_ERR,"get_compression_stats() failed, rc = %d: %s\n",
                comp_rc,sstrerror(serrno));
        }
    }
#endif /* !_WIN32 && !(_AIX && !ADSTAR) */
    (void) close(fd) ; 
    filereq->TEndTransferTape = (int)time(NULL);
    return(rc);
}

/*
 * Closing tape file on error.
 */
int tcloserr(int fd, tape_list_t *tape, file_list_t *file) {
    int rc = 0;
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

    if ( tapereq->mode == WRITE_ENABLE ) {
        if ( file->trec > 0 ) rtcp_log(LOG_INFO,"tcloserr(%d) delete tape file, tape path %s\n",
            fd,filereq->tape_path);
        if ( file->trec>0 && (rc = deltpfil(fd,filereq->tape_path)) < 0 ) {
            rtcpd_SetReqStatus(NULL,file,-rc,RTCP_FAILED | RTCP_SYERR);
            rtcpd_AppendClientMsg(NULL,file, RT141, "CPDSKTP");
            rtcp_log(LOG_ERR,RT141,"CPDSKTP");
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
    int     rc ;
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
            if ( wrthdrlbl(fd,filereq->tape_path) < 0 ) {
#if defined(_IBMR2)
                if ( errno == ENOSPC || errno == ENXIO)
#else
                if ( errno == ENOSPC )
#endif
                    serrno = ENOSPC;
                else
                    twerror(fd,tape,file) ;
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
            if ( mt_info.mt_erreg == 19 ) { /* SC_EOT */
#else
            if ( mt_info.mt_erreg == 0 ) {
#endif  /* sun */
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
        return (write_sony(fd, ptr, len));
    }
  return(rc);
}

/*
 * Reading tape block.
 */
int tread(int fd,char *ptr,int len,
           tape_list_t *tape, file_list_t *file) {
    int     rc, status;
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
            char *msgaddr;

            if (gettperror (fd, &msgaddr) == ETBLANK) {
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
            }
            return(-1);
        }
#endif  /* sun  */
    
        /*
         * Returning number of bytes read.
         */
        if ( rc == 0 ) {
            rtcp_log(LOG_DEBUG,"tread(%d,%s) check EOF/EOV\n",fd,filereq->tape_path);
            if ((rc = checkeofeov(fd, filereq->tape_path)) < 0) {
                if (rc == -ETLBL) {
                    rtcpd_AppendClientMsg(NULL, file, RT128,"CPTPDSK");
                    rtcpd_SetReqStatus(NULL,file,-rc,
                        RTCP_FAILED | RTCP_USERR);
                    rtcp_log(LOG_ERR,RT128,"CPTPDSK");
                } else
                    rtcpd_SetReqStatus(NULL,file,-rc,
                    RTCP_FAILED | RTCP_UNERR);
                return(-1);
            }
            if ( rc == 0 ) return(0);   /* end of file has been reached */
            file->eovflag= 1;           /* tape volume overflow */
            return(0);
        } else
            return(rc);
    } else {
      return (read_sony(fd, ptr, len));
    }
}

/*  tpio_sony - sends scsi commands to support RAW mode on SONY DIR1000
    using a DFC-1500/1700 controller */

static int read_sony(int fd, char *buf, int len) {
#if SONYRAW
    unsigned char cdb[6];
    int errcat;
    int flags;
    char *msgaddr;
    int n;
    int nb_sense_ret;
    int ntracks;
    int offset;
    int rc;
    char sense[MAXSENSE];
    int tpt;

    ntracks = len / 144432;
#if IRIX6
    tpt = 29;
#else
#if HPUX10
    tpt = 7;    /* Tracks per scsi transfer */
#else
    tpt = 1;
#endif
#endif
    offset = 0;
    trec++;
    flags = SCSI_IN | SCSI_SEL_WITH_ATN;
    if (! negotiate) {  /* Set Fast/Wide on first xfer */
        flags |= (SCSI_SYNC | SCSI_WIDE);
        negotiate = 1;
    }
    while (ntracks > 0) {
        memset (cdb, 0, sizeof(cdb));
        cdb[0] = 0x08;      /* Read */
        cdb[1] = 0x01;      /* Fixed blocks */
        n = (ntracks > tpt) ? tpt : ntracks;
        cdb[4] = n;
        rc = send_scsi_cmd (fd, "", 1, cdb, 6, buf + offset, n * 144432,
            sense, 38, 30000, flags, &nb_sense_ret, &msgaddr);
        if (rc == -1 || rc == -2) {
            (void) fprintf (stderr, RT113, "CPTPDSK", msgaddr, trec);
            exit (UNERR);
        }
        if (rc < 0) {
            (void) fprintf (stderr, RT125, "CPTPDSK", msgaddr, trec);
            if (rc == -4 && nb_sense_ret >= 14 &&
                get_sk_msg (sense[2] & 0xF, sense[12], sense[13], &msgaddr) == ETPARIT)
                exit (USERR);
            else
                exit (SYERR);
        }
        ntracks -= n;
        offset += n * 144432;
    }
    return (len);
#else /* SONYRAW */
    return(-1);
#endif /* SONYRAW */
}
        
static int write_sony(int fd, char *buf, int len) {
#if defined(SONYRAW)
    unsigned char cdb[6];
    int errcat;
    int flags;
    char *msgaddr;
    int n;
    int nb_sense_ret;
    int ntracks;
    int offset;
    int rc;
    char sense[MAXSENSE];
    int tpt;

    ntracks = len / 144432;
#if IRIX6
    tpt = 29;
#else
#if HPUX10
    tpt = 7;    /* Tracks per scsi transfer */
#else
    tpt = 1;
#endif
#endif
    offset = 0;
    trec++;
    flags = SCSI_OUT | SCSI_SEL_WITH_ATN;
    if (! negotiate) {  /* Set Fast/Wide on first xfer */
        flags |= (SCSI_SYNC | SCSI_WIDE);
        negotiate = 1;
    }
    while (ntracks > 0) {
        memset (cdb, 0, sizeof(cdb));
        cdb[0] = 0x0A;      /* Write */
        cdb[1] = 0x01;      /* Fixed blocks */
        n = (ntracks > tpt) ? tpt : ntracks;
        cdb[4] = n;
        rc = send_scsi_cmd (fd, "", 1, cdb, 6, buf + offset, n * 144432,
            sense, 38, 30000, flags, &nb_sense_ret, &msgaddr);
        if (rc == -1 || rc == -2) {
            (void) fprintf (stderr, RT116, "CPDSKTP", msgaddr, trec);
            exit (UNERR);
        }
        if (rc < 0) {
            (void) fprintf (stderr, RT126, "CPDSKTP", msgaddr, trec);
            if (rc == -4 && nb_sense_ret >= 14 &&
                get_sk_msg (sense[2] & 0xF, sense[12], sense[13], &msgaddr) == ETPARIT)
                exit (USERR);
            else
                exit (SYERR);
        }
        ntracks -= n;
        offset += n * 144432;
    }
    return (len);
#else /* SONYRAW */
    return(-1);
#endif /* SONYRAW */
}
