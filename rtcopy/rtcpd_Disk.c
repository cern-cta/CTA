/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_Disk.c,v $ $Revision: 1.108 $ $Date: 2004/09/24 13:09:36 $ CERN-IT/ADC Olof Barring";
#endif /* not lint */

/*
 * rtcpd_Disk.c - RTCOPY server disk IO thread 
 */

#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <stdio.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <io.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#endif /* _WIN32 */

#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/stat.h>

#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <sacct.h>
#define RFIO_KERNEL 1
#include <rfio.h>
#include <rfio_errno.h>
#include <Cthread_api.h>
#include <Cpool_api.h>
#include <vdqm_api.h>
#include <Ctape_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <serrno.h>
#include <u64subr.h>

#define DK_STATUS(X) (diskIOstatus->current_activity = (X))
#define DK_SIZE(X)   (diskIOstatus->nbbytes = (X))
#define DEBUG_PRINT(X) {if ( debug == TRUE ) rtcp_log X ;}

extern char *getifnam _PROTO((SOCKET));

typedef struct thread_arg {
    SOCKET client_socket;
    int pool_index;              /* Pool index of this thread */
    int start_indxp;             /* Index of start buffer */
    int start_offset;            /* Byte offset withint start buffer */
    int last_file;               /* TRUE if this is the last disk file */
    int end_of_tpfile;           /* TRUE if last buffer for this file
                                  * should be marked end_to_tpfile=TRUE */
    tape_list_t *tape;
    file_list_t *file;
    rtcpClientInfo_t *client;
} thread_arg_t;

thread_arg_t *thargs = NULL;

extern int Debug;

extern int nb_bufs;

extern int bufsz;

extern processing_cntl_t proc_cntl;

extern processing_status_t proc_stat;

extern buffer_table_t **databufs;

extern rtcpClientInfo_t *client;

extern int AbortFlag;

static int FortranUnitTable[NB_FORTRAN_UNITS];  /* Table of Fortran units */

int success = 0;
int failure = -1;

static int DiskIOstarted() {
    int rc;

    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"DiskIOstarted(): Cthread_mutex_lock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    proc_cntl.diskIOstarted = 1;
    rtcp_log(LOG_DEBUG,"DiskIOstarted() nb active disk IO threads=%d\n",
             proc_cntl.nb_diskIOactive);
    rc = Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"DiskIOstarted(): Cthread_cond_broadcast_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
        return(-1);
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"DiskIOstarted(): Cthread_mutex_unlock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    return(0);
}

static void DiskIOfinished() {
    (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    proc_cntl.nb_diskIOactive--;
    rtcp_log(LOG_DEBUG,"DiskIOfinished() nb active disk IO threads=%d\n",
             proc_cntl.nb_diskIOactive);
    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    return;
}

int rtcpd_WaitForPosition(tape_list_t *tape, file_list_t *file) {
    int rc,severity;

    rtcp_log(LOG_DEBUG,"rtcpd_WaitForPosition() called\n");
    if ( tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    if ( *file->filereq.recfm != '\0' && (tape->tapereq.mode == WRITE_ENABLE ||
         *file->filereq.stageID == '\0') ) return(0);

    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_WaitForPosition(): Cthread_mutex_lock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    while ( file->filereq.proc_status == RTCP_WAITING ) {
        rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
        if ( ((severity | rtcpd_CheckProcError()) &
              (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV|RTCP_EOD)) != 0 ) {
            (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
            return(0);
        }
        rc = Cthread_cond_wait_ext(proc_cntl.cntl_lock);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_WaitForPosition(): Cthread_cond_broadcast_ext(proc_cntl) : %s\n", 
                sstrerror(serrno));
            (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
            return(-1);
        }
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_WaitForPosition(): Cthread_mutex_lock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_WaitForPosition() returns\n");
    return(0);
}

/*
 * Disk I/O wrappers
 */
static int GetNewFortranUnit(int pool_index, file_list_t *file) {
    rtcpFileRequest_t *filereq;
    int AssignedUnit, i, rc;
    diskIOstatus_t *diskIOstatus = NULL;

    if ( file == NULL ) return(-1);
    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    filereq = &file->filereq;
    /*
     * Lock the Fortran Unit table and assign a new number.
     * Note that we use the proc_cntl lock in order to not
     * have too many differnt locks. The overhead should be
     * minimal since this routine is normally not called very 
     * frequently
     */
    DK_STATUS(RTCP_PS_WAITMTX);
    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    DK_STATUS(RTCP_PS_NOBLOCKING);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"GetNewFortranUnit(): Cthread_mutex_lock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    AssignedUnit = -1;
    for (i=0;i<NB_FORTRAN_UNITS;i++) {
        if ( FortranUnitTable[i] == FALSE ) {
            /*
             * Unit not is use. Grab it.
             */
            AssignedUnit = i;
            FortranUnitTable[i] = TRUE;
            break;
        }
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"GetNewFortranUnit(): Cthread_mutex_unlock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    if ( AssignedUnit == -1 ) {
        file->FortranUnit = AssignedUnit;
        return(-1);
    } else {
        file->FortranUnit = FORTRAN_UNIT_OFFSET + AssignedUnit;
        return(0);
    }
}

static int ReturnFortranUnit(int pool_index, file_list_t *file) {
    rtcpFileRequest_t *filereq;
    int rc;
    diskIOstatus_t *diskIOstatus = NULL;

    if ( file == NULL ) return(-1);
    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    filereq = &file->filereq;
    if ( !VALID_FORTRAN_UNIT(file->FortranUnit) ) return(0);
    /*
     * Lock the Fortran Unit table and return the unit.
     * Note that we use the proc_cntl lock in order to not
     * have too many differnt locks. The overhead should be
     * minimal since this routine is normally not called very 
     * frequently
     */
    DK_STATUS(RTCP_PS_WAITMTX);
    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    DK_STATUS(RTCP_PS_NOBLOCKING);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"ReturnFortranUnit(): Cthread_mutex_lock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    FortranUnitTable[file->FortranUnit] = FALSE;
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"ReturnFortranUnit(): Cthread_mutex_unlock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    file->FortranUnit = -1;
    return(0);
}
    
static int LockForAppend(const int lock) {
    int rc,severity,i;
    static int nb_waiters = 0;
    static int next_entry = 0;
    static int *wait_list = NULL;

    rtcp_log(LOG_DEBUG,"LockForAppend(%d) current_lock=%d\n",
             lock,proc_cntl.DiskFileAppend);
    return(rtcpd_SerializeLock(lock,&proc_cntl.DiskFileAppend,
           proc_cntl.DiskFileAppend_lock,&nb_waiters,&next_entry,&wait_list));
}

static int DiskFileOpen(int pool_index, 
                        tape_list_t *tape,
                        file_list_t *file) {
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    int rc, irc, save_errno, save_serrno, save_rfio_errno;
    int disk_fd, flags, use_rfioV3, severity;
    diskIOstatus_t *diskIOstatus = NULL;
    char *ifce;
    char node[2];
    SOCKET s;
    char Uformat_flags[8];
    register int debug = Debug;
#if defined(_WIN32)
    int binmode = O_BINARY;
#else /* _WIN32 */
    int binmode = 0;
#endif /* _WIN32 */

    rc = irc = 0;
    disk_fd = -1;
    if ( tape == NULL || file == NULL ) return(-1);

    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    tapereq = &tape->tapereq;
    filereq = &file->filereq;
    umask((mode_t)filereq->umask);

    /*
     * If this file is concatenating or is going to be
     * concatenated we must serialize the access to it.
     */
    if ( (tapereq->mode == WRITE_DISABLE) && 
         ( (file->next->filereq.concat & CONCAT) != 0 ||
           (filereq->concat & (CONCAT|CONCAT_TO_EOD)) != 0 )) {
        rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
        if ( (severity & RTCP_EOD) != 0 ) return(-1);
        log(LOG_DEBUG,"DiskFileOpen(%s) lock file for concatenation\n",
            filereq->file_path);
        rc = LockForAppend(1);
        rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
        if ( rc == -1 ) {
             rtcp_log(LOG_ERR,"DiskFileOpen(%s) LockForAppend(0): %s\n",
                     filereq->file_path,sstrerror(serrno));
            return(-1);
        }
        if ( (severity & RTCP_EOD) != 0 ) return(-1);
    }

    /*
     * Open the disk file
     */
    if ( (*filereq->recfm == 'F') || ((filereq->convert & NOF77CW) != 0) ) {
        /*
         * Normal Formatted file option. 
         * Open the disk file as byte-stream.
         */
        flags = O_RDONLY | binmode;
        if ( tapereq->mode == WRITE_DISABLE ) {
            if ( (filereq->concat & (CONCAT | CONCAT_TO_EOD)) != 0 ) {
                /*
                 * Appending to previous disk file. If concat to EOD we
                 * must check if it is the first file in sequence so that
                 * we don't append to an existing file....
                 */
                if ( (filereq->concat & CONCAT_TO_EOD) != 0 &&
                     (filereq->concat & CONCAT) == 0 &&
                     ((file->prev->filereq.concat & CONCAT_TO_EOD) == 0 ||
                      file == tape->file) )
                    flags = O_CREAT | O_WRONLY | O_TRUNC | binmode;
                else
                    flags = O_CREAT | O_WRONLY | O_APPEND | binmode;
            } else {
                /*
                 * New disk file unless an offset has been specified.
                 */
                if ( filereq->offset > 0 ) 
                    flags = O_CREAT| O_WRONLY | binmode;
                else 
                    flags = O_CREAT | O_WRONLY | O_TRUNC | binmode;
            }
        }
        /* Activate new transfer mode for source file */
        use_rfioV3 = RFIO_STREAM;
        rfiosetopt(RFIO_READOPT,&use_rfioV3,4); 

        rfio_errno = 0;
        serrno = 0;
        errno = 0;
        rtcp_log(LOG_DEBUG,"DiskFileOpen() open(%s,0x%x)\n",filereq->file_path,
            flags);
        DK_STATUS(RTCP_PS_OPEN);
        rc = rfio_open64(filereq->file_path,flags,0666);
        DK_STATUS(RTCP_PS_NOBLOCKING);
        if ( rc == -1 ) {
            save_errno = errno;
            save_serrno = serrno;
            save_rfio_errno = rfio_errno;
            rtcp_log(LOG_ERR,
                "DiskFileOpen() rfio_open64(%s,0x%x): errno = %d, serrno = %d, rfio_errno = %d\n",
                filereq->file_path,flags,errno,serrno,rfio_errno);
        } else {
            disk_fd = rc;
            rc = 0;
        }
        rtcp_log(LOG_DEBUG,"DiskFileOpen() rfio_open() returned fd=%d\n",
            disk_fd);
        if ( rc == 0 && filereq->offset > 0 ) {
			char tmpbuf[21];
			char tmpbuf2[21];
			off64_t rc64;
            rtcp_log(LOG_DEBUG,"DiskFileOpen() attempt to set offset %s\n",
                     u64tostr((u_signed64) filereq->offset, tmpbuf, 0));
            rfio_errno = 0;
            serrno = 0;
            errno = 0;
            rc64 = rfio_lseek64(disk_fd,(off64_t)filereq->offset,SEEK_SET);
            if ( rc64 == -1 ) {
                save_errno = errno;
                save_serrno = serrno;
                save_rfio_errno = rfio_errno;
                rtcp_log(LOG_ERR,
                 "DiskFileOpen() rfio_lseek64(%d,%s,0x%x): errno = %d, serrno = %d, rfio_errno = %d\n",
                 disk_fd,u64tostr((u_signed64)filereq->offset,tmpbuf,0),SEEK_SET,errno,serrno,rfio_errno);
				rc = -1;
            } else if ( rc64 != (off64_t)filereq->offset ) {
                save_errno = errno;
                save_serrno = serrno;
                save_rfio_errno = rfio_errno;
                rtcp_log(LOG_ERR,"rfio_lseek64(%d,%s,%d) returned %s\n",
                         disk_fd,u64tostr((u_signed64)filereq->offset,tmpbuf,0),SEEK_SET,u64tostr((u_signed64)rc64,tmpbuf2,0));
                if ( save_rfio_errno == 0 && save_serrno == 0 &&
                     save_errno == 0 ) save_rfio_errno = SEINTERNAL;
                rc = -1;
            } else rc = 0;
        }
    } else if ( (*filereq->recfm == 'U') && 
                ((filereq->convert & NOF77CW) == 0) ) {
        /*
         * FORTRAN sequential file access. Disk file contains FORTRAN
         * control words
         */
        rc = GetNewFortranUnit(pool_index,file);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"DiskFileOpen() GetNewFortranUnit() failed\n");
            serrno = SEINTERNAL;
            if ( tapereq->mode == WRITE_ENABLE ) 
                rtcpd_AppendClientMsg(NULL, file,RT110,"CPDSKTP",
                                      sstrerror(serrno));
            else
                rtcpd_AppendClientMsg(NULL, file,RT110,"CPTPDSK",
                                      sstrerror(serrno));
            return(-1);
        }
        strcpy(Uformat_flags,"US");
        if ( tapereq->mode == WRITE_DISABLE ) {
            if ( (filereq->concat & (CONCAT | CONCAT_TO_EOD)) != 0 ) {
                /*
                 * Appending to previous disk file. 
                 */
                strcat(Uformat_flags,"EA");
            } else {
                /*
                 * New disk file
                 */
                strcat(Uformat_flags,"E");
            }
        }
        strcpy(node," ");
        rfio_errno = 0;
        serrno = 0;
        errno = 0;
        DK_STATUS(RTCP_PS_OPEN);
        rc = rfio_xyopen(filereq->file_path,node,file->FortranUnit,0,
                         Uformat_flags,&irc);
        DK_STATUS(RTCP_PS_NOBLOCKING);
        if ( rc != 0 || irc != 0 ) {
            save_errno = errno;
            save_serrno = serrno;
            save_rfio_errno = (rc != 0 ? rc : irc);
            rtcp_log(LOG_ERR,
                "DiskFileOpen() rfio_xyopen(%s,%d): errno = %d, serrno = %d, rfio_errno = %d\n",
                filereq->file_path,Uformat_flags,errno,serrno,rfio_errno);
        } else {
            disk_fd = file->FortranUnit;
        }
    } else {
        rtcp_log(LOG_ERR,"DiskFileOpen() unknown recfm %s + convert 0x%x\n",
            filereq->recfm,filereq->convert);
        serrno = EINVAL;
        return(-1);
    }
    if ( rc != 0 || irc != 0 ) {
        if ( tapereq->mode == WRITE_ENABLE ) 
            rtcpd_AppendClientMsg(NULL, file,RT110,"CPDSKTP",
                                  rfio_serror());
        else
            rtcpd_AppendClientMsg(NULL, file,RT110,"CPTPDSK",
                                  rfio_serror());
        switch (save_rfio_errno) {
        case ENOENT:
        case EISDIR:
        case EPERM:
        case EACCES:
            rtcpd_SetReqStatus(NULL,file,save_rfio_errno,RTCP_USERR | RTCP_FAILED);
            break;
        default:
            if (save_errno = EBADF && save_rfio_errno == 0 && save_serrno == 0)
                rtcpd_SetReqStatus(NULL,file,save_rfio_errno,
                                   RTCP_RESELECT_SERV | RTCP_UNERR);
            else if ( save_serrno = SETIMEDOUT && save_rfio_errno == 0 &&
                      filereq->err.max_cpretry > 0 ) {
                filereq->err.max_cpretry--;
                rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_RESELECT_SERV|
                                                         RTCP_UNERR);
            } else {
                save_rfio_errno = (save_rfio_errno > 0 ? save_rfio_errno :
                                                         save_serrno);
                save_rfio_errno = (save_rfio_errno > 0 ? save_rfio_errno :
                                                         save_errno); 
                rtcpd_SetReqStatus(NULL,file,save_rfio_errno,
                                   RTCP_FAILED | RTCP_UNERR);
            }
            break;
        }
        return(-1);
    }
    if ( disk_fd != -1 ) {
        /*
         * Note: this works as long as rfio_open() returns a socket.
         * If we implement internal file descriptor tables in RFIO
         * client we must change this to call a routine to
         * return the socket.
         */
        s = (SOCKET)disk_fd;
        if ( (*filereq->recfm == 'U') && ((filereq->convert & NOF77CW) == 0) )
            s = (SOCKET)rfio_xysock(disk_fd);
        ifce = getifnam(s);
        if ( ifce == NULL )
            strcpy(filereq->ifce,"???");
        else
            strcpy(filereq->ifce,ifce);
    }

    return(disk_fd);
}

static int DiskFileClose(int disk_fd,
                         int pool_index,
                         tape_list_t *tape,
                         file_list_t *file) {
    rtcpFileRequest_t *filereq;
    int rc, irc, save_rfio_errno, save_serrno, save_errno;
    diskIOstatus_t *diskIOstatus = NULL;

    if ( file == NULL ) return(-1);
    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    filereq = &file->filereq;

    rtcp_log(LOG_DEBUG,"DiskFileClose(%s) close file descriptor %d\n",
             filereq->file_path,disk_fd);

    save_rfio_errno = rfio_errno;
    save_serrno = serrno;
    serrno = rfio_errno = 0;
    if ( (*filereq->recfm == 'F') || ((filereq->convert & NOF77CW) != 0) ) {
        rc = rfio_close(disk_fd);
        save_errno = errno;
        save_serrno = serrno;
        save_rfio_errno = rfio_errno;
    } else if ( (*filereq->recfm == 'U') && 
                ((filereq->convert & NOF77CW) == 0) ) {
        rc = rfio_xyclose(file->FortranUnit," ",&irc);
        save_errno = errno;
        save_serrno = serrno;
        save_rfio_errno = rfio_errno;
        (void)ReturnFortranUnit(pool_index,file);
    }
    if ( rc == -1 ) {
        rtcpd_AppendClientMsg(NULL, file,RT108,"CPTPDSK",rfio_serror());

        rtcp_log(LOG_ERR,"%s: errno = %d, serrno = %d, rfio_errno = %d\n",
                 (((*filereq->recfm == 'F') || 
                  ((filereq->convert & NOF77CW) != 0) ) ? 
                 "rfio_close()" : "rfio_xyclose()"),
                 save_errno,save_serrno,save_rfio_errno);
        if ( save_rfio_errno == ENOSPC || (save_rfio_errno == 0 &&
                                           save_errno == ENOSPC) ) {
            save_rfio_errno = ENOSPC;
            rtcp_log(LOG_DEBUG,"DiskFileClose(%s) ENOSPC detected\n",
                                  filereq->file_path);
            if ( *filereq->stageID != '\0' ) {
                rtcp_log(LOG_DEBUG,"DiskFileClose(%s) stageID=<%s>, request local retry\n",
                          filereq->file_path,filereq->stageID);
                rtcpd_SetReqStatus(NULL,file,save_rfio_errno,RTCP_LOCAL_RETRY);
            } else {
                rtcpd_SetReqStatus(NULL,file,save_rfio_errno,RTCP_FAILED);
            }
        } else {
            save_rfio_errno = (save_rfio_errno > 0 ? save_rfio_errno : 
                                                     save_serrno);
            save_rfio_errno = (save_rfio_errno > 0 ? save_rfio_errno :
                                                     save_errno);
            rtcpd_SetReqStatus(NULL,file,save_rfio_errno,RTCP_FAILED);
        }
    } else {
        serrno = save_serrno;
        rfio_errno = save_rfio_errno;
    }

    if ( (tape->tapereq.mode == WRITE_DISABLE) &&
         ( (file->next->filereq.concat & CONCAT) != 0 ||
           (filereq->concat & (CONCAT|CONCAT_TO_EOD)) != 0 ) ) {
        rtcp_log(LOG_DEBUG,"DiskFileClose(%s) unlock file for concatenation\n",
                 filereq->file_path);
        rc = LockForAppend(0);
        if ( rc == -1 && serrno > 0 ) {
            rtcp_log(LOG_ERR,"DiskFileClose(%s) LockForAppend(0): %s\n",
                     filereq->file_path,sstrerror(serrno));
        }
    }
    filereq->TEndTransferDisk = (int)time(NULL);

    return(rc);
}

/*
 * Copy routines
 */
static int MemoryToDisk(int disk_fd, int pool_index,
                        int *indxp, int *offset,
                        int *last_file, int *end_of_tpfile,
                        tape_list_t *tape,
                        file_list_t *file) {
    int rc, irc, status, i, j, blksiz, lrecl, save_serrno;
    int buf_done, nb_bytes, proc_err, severity, last_errno, SendStartSignal;
    register int Uformat;
    register int debug = Debug;
    register int convert, concat;
    char *convert_buffer = NULL;
    char errmsgtxt[80] = {""};
    void *f77conv_context = NULL;
    diskIOstatus_t *diskIOstatus = NULL;
    char *bufp, save_rfio_errmsg[CA_MAXLINELEN+1];
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;

    if ( disk_fd < 0 || indxp == NULL || offset == NULL ||
         last_file == NULL || end_of_tpfile == NULL ||
         tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    tapereq = &tape->tapereq;
    filereq = &file->filereq;
    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    diskIOstatus->disk_fseq = filereq->disk_fseq;
    blksiz = lrecl = -1;
    Uformat = (*filereq->recfm == 'U' ? TRUE : FALSE);
    convert = filereq->convert;
    concat = filereq->concat;

    /*
     * Main write loop. End with EOF on tape file or error condition
     */
    *end_of_tpfile = FALSE;
    file->diskbytes_sofar = filereq->startsize;
    DK_SIZE(file->diskbytes_sofar);
    save_serrno = 0;
    proc_err = 0;
    SendStartSignal = TRUE;
    for (;;) {
        i = *indxp;
        buf_done = FALSE;
        /*
         * Synchronize access to next buffer
         */
        DK_STATUS(RTCP_PS_WAITMTX);
        rc = Cthread_mutex_lock_ext(databufs[i]->lock);
        DK_STATUS(RTCP_PS_NOBLOCKING);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"MemoryToDisk() Cthread_mutex_lock_ext(): %s\n",
                sstrerror(serrno));
            if ( convert_buffer != NULL ) free(convert_buffer);
            if ( f77conv_context != NULL ) free(f77conv_context);
            return(-1);
        }
        /*
         * Wait until it is full
         */
        while ( databufs[i]->flag == BUFFER_EMPTY ) {
            rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
            if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
                  (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                if ( convert_buffer != NULL ) free(convert_buffer);
                if ( f77conv_context != NULL ) free(f77conv_context);
                break;
            }

            DEBUG_PRINT((LOG_DEBUG,"MemoryToDisk() wait on buffer[%d]->flag=%d\n",
                    i,databufs[i]->flag));
            databufs[i]->nb_waiters++;
            DK_STATUS(RTCP_PS_WAITCOND);
            rc = Cthread_cond_wait_ext(databufs[i]->lock);
            DK_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToDisk() Cthread_cond_wait_ext(): %s\n",
                    sstrerror(serrno));
                if ( convert_buffer != NULL ) free(convert_buffer);
                if ( f77conv_context != NULL ) free(f77conv_context);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }

            databufs[i]->nb_waiters--;

        } /* while (databufs[i]->flag == BUFFER_EMPTY) */

        if ( databufs[i]->flag == BUFFER_FULL ) {
            rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
            if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
                (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                if ( convert_buffer != NULL ) free(convert_buffer);
                if ( f77conv_context != NULL ) free(f77conv_context);
            }
        }

        /*
         * At this point we know that the tape->memory has
         * started. Thus it is now safe to take the blocksize and
         * record length. Before this was not the case because
         * for tape read the call to Ctape_info() in the
         * tape IO thread is not synchronised with the start up
         * of the disk IO thread.
         */
        if ( blksiz < 0 ) {
            blksiz = filereq->blocksize;
            lrecl = filereq->recordlength;
            if ( (lrecl <= 0) && ((Uformat == FALSE) ||
                 ((convert & NOF77CW) != 0)) ) lrecl = blksiz;
            else if ( (Uformat == TRUE) &&
                      ((convert & NOF77CW) == 0) ) lrecl = 0;
            filereq->TStartTransferDisk = (int)time(NULL);
        }

        /*
         * Check if reached an allowed end-of-tape
         */
        if ( (proc_err & 
              (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV)) == 0 && 
             (concat & (NOCONCAT_TO_EOD|CONCAT_TO_EOD)) != 0 ) { 
            rtcpd_CheckReqStatus(file->tape,file,NULL,&proc_err);
            if ( (proc_err = (proc_err & RTCP_EOD)) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            }
        }
        if ( proc_err != 0 ) break;
        DEBUG_PRINT((LOG_DEBUG,"MemoryToDisk() buffer %d full\n",i));
        if ( SendStartSignal == TRUE ) {
            /*
             * Signal to StartDiskIO() that we are starting to empty the
             * first buffer.
             */
            DK_STATUS(RTCP_PS_WAITMTX);
            rc = DiskIOstarted();
            DK_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
            SendStartSignal = FALSE;
        }

        /*
         * Should never happen unless there is a bug.
         */
        if ( (databufs[i]->data_length > databufs[i]->maxlength) ||
             (databufs[i]->data_length > databufs[i]->length) ) {
            rtcp_log(LOG_ERR,"Buffer overflow!! databuf %d, (%d,%d,%d)\n",
                     i,databufs[i]->data_length,databufs[i]->length,
                     databufs[i]->maxlength);
            rtcpd_AppendClientMsg(NULL,file,"Internal error. %s: buffer overflow\n",
                                  filereq->file_path);
            rtcpd_SetReqStatus(NULL,file,SEINTERNAL,RTCP_FAILED);
            (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
            (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            if ( convert_buffer != NULL ) free(convert_buffer);
            if ( f77conv_context != NULL ) free(f77conv_context);
            return(-1);
        }
        /*
         * Verify that actual buffer size matches block size
         */
        if ( (databufs[i]->length % blksiz) != 0 || blksiz < 0 ) {
            rtcp_log(LOG_ERR,"MemoryToDisk() blocksize mismatch\n");
            rtcpd_AppendClientMsg(NULL, file, "Internal error. %s: blocksize mismatch (%d,%d)\n",
                        filereq->file_path,databufs[i]->length,blksiz);
            rtcpd_SetReqStatus(NULL,file,SEINTERNAL,RTCP_FAILED);
            (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
            (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            if ( convert_buffer != NULL ) free(convert_buffer);
            if ( f77conv_context != NULL ) free(f77conv_context);
            return(-1);
        }
        /*
         * Allocate conversion buffer if necessary
         */
        if ( ((convert & FIXVAR) != 0) && (convert_buffer == NULL) ) {
            convert_buffer = (char *)malloc(databufs[i]->length +
                (databufs[i]->length + lrecl -1)/lrecl);
            if ( convert_buffer == NULL ) {
                (void)rtcpd_SetReqStatus(NULL,file,errno,
                                         RTCP_RESELECT_SERV);
                (void)rtcpd_AppendClientMsg(NULL,file,RT105,sstrerror(errno));
                rtcp_log(LOG_ERR,"MemoryToDisk() malloc(): %s\n",
                    sstrerror(errno));
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                if ( f77conv_context != NULL ) free(f77conv_context);
                return(-1);
            }
        }
        /*
         * Check if this is the last buffer of the tape file
         * which implies that we should return.
         */
        *end_of_tpfile = databufs[i]->end_of_tpfile;
        /*
         * Copy the data from memory to disk. Note that offset should
         * always be zero for tape->disk copy because new files will
         * begin with a new buffer.
         */
        if ( databufs[i]->data_length > 0 ) {
            nb_bytes = databufs[i]->data_length;
            /*
             * Check that the total size does not exceed maxsize specified
             * by user. If so, reduce the nb bytes accordingly. The tape IO
             * should already have stopped in this buffer but data_length
             * is a multiple of blocksize and may therefore exceed maxsize.
             */
            if ( (filereq->maxsize > 0) && (file->diskbytes_sofar + 
                  (u_signed64)nb_bytes > filereq->maxsize) ) {
                nb_bytes = (int)(filereq->maxsize - file->diskbytes_sofar);
                databufs[i]->data_length = nb_bytes;
            }
            if ( nb_bytes > 0 ) {
                /*
                 * Convert from EBCDIC to ASCII character coding? 
                 */
                if ( (convert & EBCCONV) != 0 ) 
                    ebc2asc(databufs[i]->buffer,nb_bytes);
                /*
                 * >>>>>>>>>>> write to disk <<<<<<<<<<<<<
                 */
                if ( (Uformat == FALSE) || ((convert & NOF77CW) != 0) ) {
                    bufp = databufs[i]->buffer;
                    if ( (convert & FIXVAR) != 0 ) {
                        nb_bytes = rtcpd_FixToVar(bufp,
                            convert_buffer,nb_bytes,lrecl);
                        bufp = convert_buffer;
                    } else if ( (Uformat == FALSE) &&
                                ((convert & NOF77CW) != 0) ) {
                        /*
                         * F,-f77 format
                         */
                        nb_bytes = rtcpd_f77RecToFix(bufp,
                            nb_bytes,lrecl,errmsgtxt,&f77conv_context);
                        if ( *errmsgtxt != '\0' ) 
                            rtcpd_AppendClientMsg(NULL, file,errmsgtxt);
                    }
                    DK_STATUS(RTCP_PS_WRITE);
                    if ( nb_bytes > 0 ) rc = rfio_write(disk_fd,bufp,nb_bytes);
                    else rc = nb_bytes;
                    DK_STATUS(RTCP_PS_NOBLOCKING);
                    if ( rc == -1 || rc != nb_bytes ) {
                        last_errno = errno;
                        save_serrno = rfio_serrno();
                        rtcp_log(LOG_ERR,"rfio_write(): errno = %d, serrno = %d, rfio_errno = %d\n",last_errno,serrno,save_serrno);
                    }
                } else {
                    /*
                     * All U format except U,bin 
                     */
                    rc = 0;
                    for (j=0; j*blksiz < nb_bytes; j++) {
                        lrecl = databufs[i]->lrecl_table[j];
                        bufp = databufs[i]->buffer + j*blksiz;
                        DK_STATUS(RTCP_PS_READ);
                        status = rfio_xywrite(file->FortranUnit,bufp,0,
                                         lrecl," ",&irc);
                        DK_STATUS(RTCP_PS_NOBLOCKING);
                        if ( status != 0 || irc != 0 ) {
                            last_errno = errno;
                            save_serrno = rfio_serrno();
                            rtcp_log(LOG_ERR,"rfio_xywrite(): errno = %d, serrno = %d, rfio_errno = %d\n",last_errno,serrno,save_serrno);
                            if ( status == ENOSPC || irc == ENOSPC )
                                save_serrno = ENOSPC;
                            rc = -1;
                            break;
                        } else rc += lrecl;
                    }
                    lrecl = 0;
                }
                if ( rc != nb_bytes ) {
                    strncpy(save_rfio_errmsg,rfio_serror(),CA_MAXLINELEN);
                    /*
                     * In case of ENOSPC we will have to return
                     * to ask the stager for a new path
                     */
                    rtcpd_AppendClientMsg(NULL, file,RT115,"CPTPDSK",
                            save_rfio_errmsg);

                    if ( save_serrno == ENOSPC || (save_serrno == 0 &&
                                                   last_errno == ENOSPC) ) {
                         save_serrno = ENOSPC;
                         rtcp_log(LOG_DEBUG,"MemoryToDisk(%s) ENOSPC detected\n",
                                  filereq->file_path);
                         if ( *filereq->stageID != '\0' ) {
                             rtcp_log(LOG_DEBUG,"MemoryToDisk(%s) stageID=<%s>, request local retry\n",filereq->file_path,filereq->stageID);
                             rtcpd_SetReqStatus(NULL,file,save_serrno,
                                                RTCP_LOCAL_RETRY);
                         } else {
                             rtcpd_SetReqStatus(NULL,file,save_serrno,
                                                RTCP_FAILED);
                         }
                    }
                    if ( save_serrno != ENOSPC ) {
                        if ( last_errno == ENODEV &&
                             filereq->err.max_cpretry > 0 ) {
                            filereq->err.max_cpretry--;
                            rtcpd_SetReqStatus(NULL,file,last_errno,
                                               RTCP_LOCAL_RETRY);
                        } else {
                            rtcpd_SetReqStatus(NULL,file,save_serrno,
                                               RTCP_FAILED);
                        }
                    }
                    (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                    (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( f77conv_context != NULL ) free(f77conv_context);
                    serrno = save_serrno;
                    return(-1);
                }
            } else {
                /*
                 * Max size exceeded. Just skip over the buffer
                 * to assure that it is marked as free for the
                 * Tape IO thread. 
                 */
                rc = databufs[i]->data_length;
            }
        } else {
            rc = 0;
        }
        if ((convert & FIXVAR) == 0 ) databufs[i]->data_length -= rc;
        else databufs[i]->data_length = 0;

        file->diskbytes_sofar += (u_signed64)rc;
        DK_SIZE(file->diskbytes_sofar);
        /*
         * Reset the buffer semaphore only if the
         * full buffer has been succesfully written.
         */
        if ( databufs[i]->data_length == 0 ) {
            databufs[i]->bufendp = 0;
            databufs[i]->data_length = 0;
            databufs[i]->nbrecs = 0;
            databufs[i]->end_of_tpfile = FALSE;
            databufs[i]->last_buffer = FALSE;
            databufs[i]->flag = BUFFER_EMPTY;
            buf_done = TRUE;
        }

        /*
         * Signal and release this buffer
         */
        if ( databufs[i]->nb_waiters > 0 ) {
            rc = Cthread_cond_broadcast_ext(databufs[i]->lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToDisk() Cthread_cond_broadcast_ext(): %s\n",
                    sstrerror(serrno));
                if ( convert_buffer != NULL ) free(convert_buffer);
                if ( f77conv_context != NULL ) free(f77conv_context);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
        }
        rc = Cthread_mutex_unlock_ext(databufs[i]->lock);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"MemoryToDisk() Cthread_mutex_unlock_ext(): %s\n",
                sstrerror(serrno));
            if ( convert_buffer != NULL ) free(convert_buffer);
            if ( f77conv_context != NULL ) free(f77conv_context);
            return(-1);
        }

        if ( *end_of_tpfile == TRUE ) {
            /*
             * End of tape file reached. Close disk file and tell the
             * main control thread that we exit
             */
            DEBUG_PRINT((LOG_DEBUG,"MemoryToDisk() close disk file fd=%d\n",
                         disk_fd));
            rc = DiskFileClose(disk_fd,pool_index,tape,file);
            save_serrno = rfio_errno;
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToDisk() DiskFileClose(%d): %s\n",
                         disk_fd,rfio_serror());
                if ( convert_buffer != NULL ) free(convert_buffer);
                if ( f77conv_context != NULL ) free(f77conv_context);

                if ( save_serrno == ENOSPC ) {
                    rtcp_log(LOG_DEBUG,"MemoryToDisk(%s) ENOSPC detected\n",
                        filereq->file_path);
                    if ( *filereq->stageID != '\0' ) {
                        rtcp_log(LOG_DEBUG,"MemoryToDisk(%s) stageID=<%s>, request local retry\n",filereq->file_path,filereq->stageID);
                        rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_LOCAL_RETRY);
                    } else {
                        rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED);
                    }
                }
                serrno = save_serrno;
                return(-1);
            }
            break;
        }
        *indxp = (*indxp + 1) % nb_bufs;
        *offset = 0;

        /*
         * Has something fatal happened while we were occupied
         * reading from the disk? 
         */
        rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
        if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
              (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) break;
    } /* for (;;) */
    
    if ( convert_buffer != NULL ) free(convert_buffer);
    if ( f77conv_context != NULL ) free(f77conv_context);
    if ( proc_err != 0 ) DiskFileClose(disk_fd,pool_index,tape,file);
    return(0);
}
static int DiskToMemory(int disk_fd, int pool_index,
                        int *indxp, int *offset,
                        int *last_file, int *end_of_tpfile,
                        tape_list_t *tape,
                        file_list_t *file) {
    int rc, irc, i, j, blksiz, lrecl, end_of_dkfile, current_bufsz;
    int status, nb_bytes, SendStartSignal, save_serrno, proc_err, severity;
    register int Uformat;
    register int convert;
    register int debug = Debug;
    diskIOstatus_t *diskIOstatus = NULL;
    char *bufp, save_rfio_errmsg[CA_MAXLINELEN+1];
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;

    if ( disk_fd < 0 || indxp == NULL || offset == NULL ||
         tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    tapereq = &tape->tapereq;
    filereq = &file->filereq;
    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    diskIOstatus->disk_fseq = filereq->disk_fseq;

    blksiz = filereq->blocksize;
    lrecl = filereq->recordlength;
    Uformat = (*filereq->recfm == 'U' ? TRUE : FALSE);
    convert = filereq->convert;
    if ( (lrecl <= 0) && ((Uformat == FALSE) || ((convert & NOF77CW) != 0)) ) 
        lrecl = blksiz;
    else if ( (Uformat == TRUE) && ((convert & NOF77CW) == 0) ) lrecl = 0;
    
    /*
     * Calculate new actual buffer length
     */
    current_bufsz = rtcpd_CalcBufSz(tape,file);
    if ( current_bufsz <= 0 ) return(-1);

    /*
     * Main read loop. End with EOF or error condition (or
     * limited by size).
     */
    end_of_dkfile = FALSE;
    SendStartSignal = TRUE;
    proc_err = 0;
    file->diskbytes_sofar = filereq->startsize;
    DK_SIZE(file->diskbytes_sofar);
    filereq->TStartTransferDisk = (int)time(NULL);
    for (;;) {
        i = *indxp;
        /*
         * Synchronize access to next buffer
         */
        DK_STATUS(RTCP_PS_WAITMTX);
        rc = Cthread_mutex_lock_ext(databufs[i]->lock);
        DK_STATUS(RTCP_PS_NOBLOCKING);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"DiskToMemory() Cthread_mutex_lock_ext(): %s\n",
                sstrerror(serrno));
            return(-1);
        }
        /*
         * Wait until it is empty. 
         */
        while ( databufs[i]->flag == BUFFER_FULL ) {
            rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
            if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
                  (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                break;
            }

            databufs[i]->nb_waiters++;
            DK_STATUS(RTCP_PS_WAITCOND);
            rc = Cthread_cond_wait_ext(databufs[i]->lock);
            DK_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"DiskToMemory() Cthread_cond_wait_ext(): %s\n",
                    sstrerror(serrno));
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
            databufs[i]->nb_waiters--;

        } /* while ( databufs[i]->flag == BUFFER_FULL ) */

        if ( databufs[i]->flag == BUFFER_EMPTY ) {
            rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
            if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
                (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            }
        }

        if ( proc_err != 0 ) break;
        DEBUG_PRINT((LOG_DEBUG,"DiskToMemory() buffer %d empty\n",i));
        if ( SendStartSignal == TRUE ) {
            /*
             * Signal to StartDiskIO() that we are starting to fill the
             * first buffer. 
             */
            DK_STATUS(RTCP_PS_WAITMTX);
            rc = DiskIOstarted();
            DK_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
            SendStartSignal = FALSE;
        }
        /*
         * Set the actual buffer size to match current block size
         */
        databufs[i]->length = current_bufsz;
        /*
         * Copy the data from disk to memory
         */
        nb_bytes = databufs[i]->length-*offset;
        /*
         * Check that the total size does not exceed maxsize specified
         * by user. If so, reduce the nb bytes accordingly.
         */
        if ( (filereq->maxsize > 0) && (file->diskbytes_sofar +
              (u_signed64)nb_bytes > filereq->maxsize) ) {
            nb_bytes = (int)(filereq->maxsize - file->diskbytes_sofar);
            end_of_dkfile = TRUE;
        }
        /*
         * If true U-format, re-allocate the lrecl_table[] if needed.
         */
        if ( (Uformat == TRUE) && ((convert & NOF77CW) == 0) ) 
            rc = rtcpd_AdmUformatInfo(file,i);

        /*
         * >>>>>>>>>>> read from disk <<<<<<<<<<<<<
         */
        DEBUG_PRINT((LOG_DEBUG,"DiskToMemory() read %d bytes from %s\n",
            nb_bytes,filereq->file_path));
        rc = irc = 0;
        errno = serrno = rfio_errno = 0;
        if ( (Uformat == FALSE) || ((convert & NOF77CW) != 0) ) {
            bufp = databufs[i]->buffer + *offset;
            DK_STATUS(RTCP_PS_READ);
            if ( nb_bytes > 0 ) rc = rfio_read(disk_fd,bufp,nb_bytes);
            else rc = nb_bytes;
            DK_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) save_serrno = rfio_serrno();
        } else {
            /*
             * All U formats except U,bin
             */
            databufs[i]->nbrecs = 0;
            for (j=0; j*blksiz < nb_bytes; j++) {
                bufp = databufs[i]->buffer + j*blksiz;
                DK_STATUS(RTCP_PS_READ);
                status = rfio_xyread(file->FortranUnit,bufp,0,blksiz,
                                     &lrecl,"U",&irc);
                DK_STATUS(RTCP_PS_NOBLOCKING);
                save_serrno = rfio_serrno();
                if ( irc == 2 ) break;
                else if ( irc == SEBADFFORM ) {
                    (void)rtcpd_AppendClientMsg(NULL,file,RT106,"CPDSKTP");
                    rc = -1;
                    break;
                } else if ( irc == -1 && status == -1 ) {
                    (void)rtcpd_AppendClientMsg(NULL,file,RT123,"CPDSKTP",blksiz);
                    break;
                } else if ( irc !=0 || status != 0 ) {
                    rc = -1;
                    break;
                }
                databufs[i]->lrecl_table[j] = lrecl;
                databufs[i]->nbrecs++;
                rc += lrecl;
            }
            lrecl = 0;
        }
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"DiskToMemory() rfio_read(): errno = %d, serrno = %d, rfio_errno = %d\n",
                errno,serrno,rfio_errno);
            strncpy(save_rfio_errmsg,rfio_serror(),CA_MAXLINELEN);
            rtcpd_AppendClientMsg(NULL, file,RT112,"CPDSKTP",
                save_rfio_errmsg);
            rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED);
            (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
            (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            return(-1);
        }
        DEBUG_PRINT((LOG_DEBUG,"DiskToMemory() got %d bytes from %s\n",
            rc,filereq->file_path));
        databufs[i]->data_length += rc;
        file->diskbytes_sofar += (u_signed64)rc;
        DK_SIZE(file->diskbytes_sofar);
        if ( file->diskbytes_sofar - filereq->startsize > filereq->bytes_in ) {
            /*
             * This can happen if the user still writes to the file after
             * having submit the tape write request for it.
             */
            rtcp_log(LOG_ERR,"File %s: size changed during request!\n",
                     filereq->file_path);
            rtcpd_AppendClientMsg(NULL,file,RT150,"CPDSKTP",filereq->file_path);
            rtcpd_SetReqStatus(NULL,file,SEWOULDBLOCK,RTCP_USERR | RTCP_FAILED);
            (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
            (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            serrno = SEWOULDBLOCK;
            return(-1);
        }

        if ( (end_of_dkfile == TRUE) ||
             ((rc < nb_bytes) && 
              ((Uformat == FALSE) || ((convert & NOF77CW) != 0))) ||
             ((irc == 2) && 
              ((Uformat == TRUE) && ((convert & NOF77CW) == 0))) ) {
            DEBUG_PRINT((LOG_DEBUG,"DiskToMemory() End of file %s reached in buffer %d\n",
                filereq->file_path,i));
            if ( databufs[i]->end_of_tpfile == FALSE ) 
                databufs[i]->end_of_tpfile = *end_of_tpfile;
            databufs[i]->last_buffer = *last_file;
            if ( *offset + rc > databufs[i]->bufendp )
                databufs[i]->bufendp = *offset + rc;
            end_of_dkfile = TRUE;
        } else {
            databufs[i]->bufendp = databufs[i]->length;
        }

        /*
         * Mark the buffer as full if:
         * 1) it is full (!), i.e. data_length == length
         * 2) it is the last buffer of a tape file and
         *    all data are available (i.e. data length ==
         *    buffer end).
         * The condition 2) is complicated because of concatenation
         * of disk files into a single tape file. Note that it is
         * not enough to know that the disk IO thread for the last
         * disk file has finished (i.e. the end_of_tpfile or
         * last_file flags are set), there can be other disk IO
         * threads still active writing to another piece of this
         * buffer. This can typically happen when concatenating
         * very small disk files that all fit into a single
         * memory buffer.
         * 
         * For true U-format files we always start new files with a
         * new buffer.
         */
        if ( ((Uformat == FALSE) || ((convert & NOF77CW) != 0)) &&
            ((databufs[i]->data_length == databufs[i]->length) ||
            ((databufs[i]->data_length == databufs[i]->bufendp) &&
             (databufs[i]->end_of_tpfile == TRUE ||
              databufs[i]->last_buffer == TRUE))) ) {
            databufs[i]->flag = BUFFER_FULL;
        } else if ( (Uformat == TRUE) && ((convert & NOF77CW) == 0) ) {
            /*
             * All U formats except U,bin
             */
            databufs[i]->flag = BUFFER_FULL;
        }
        if ( databufs[i]->flag == BUFFER_FULL ) (void)rtcpd_nbFullBufs(1);
        /*
         * Signal and release this buffer
         */
        if ( databufs[i]->nb_waiters > 0 ) {
            rc = Cthread_cond_broadcast_ext(databufs[i]->lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"DiskToMemory() Cthread_cond_broadcast_ext(): %s\n",
                    sstrerror(serrno));
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
        }

        rc = Cthread_mutex_unlock_ext(databufs[i]->lock);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"DiskToMemory() Cthread_mutex_unlock_ext(): %s\n",
                sstrerror(serrno));
            return(-1);
        }

        if ( end_of_dkfile ) {
            /*
             * End of disk file reached. Close it and return to pool.
             */
            DEBUG_PRINT((LOG_DEBUG,"DiskToMemory() close disk file fd=%d\n",
                disk_fd));
            rc = DiskFileClose(disk_fd,pool_index,tape,file);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"DiskToMemory() DiskFileClose(%d), file=%s: %s\n",
                    disk_fd,filereq->file_path,rfio_serror());
                rtcpd_AppendClientMsg(NULL, file,RT108,"CPDSKTP",
                        rfio_serror());
                rtcpd_SetReqStatus(NULL,file,rfio_errno,RTCP_FAILED);
                return(-1);
            }
            break;
        }
        *indxp = (*indxp + 1) % nb_bufs;
        *offset = 0;

        /*
         * Has something fatal happened while we were occupied
         * reading from the disk?
         */
        rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
        if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
              (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) break;
    } /* for (;;) */
    
    if ( proc_err != 0 ) DiskFileClose(disk_fd,pool_index,tape,file);
    return(0);
}

/*
 * This horrible macro prevents us to always repeate the same code.
 * In addition to the return code from any blocking (or non-blocking) 
 * call we need to check the setting of the global processing error
 * to see if e.g. a tape IO thread has failed.
 * We have to be careful in setting processing error: if we are writing
 * a tape the tape IO is behind us and we have to let it finish with
 * previous file(s). Once it reach the current file it will stop because
 * the file request status is FAILED. On tape read at the other hand
 * we are behind the tape IO so we can safely interrupt everything in
 * case of an error.
 */
#define CHECK_PROC_ERR(X,Y,Z) { \
    save_errno = errno; \
    save_serrno = serrno; \
    rtcpd_CheckReqStatus((X),(Y),NULL,&severity); \
    if ( rc == -1 || (severity & (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 || \
        (rtcpd_CheckProcError() & (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 ) { \
        rtcp_log(LOG_ERR,"diskIOthread() %s, rc=%d, severity=%d, errno=%d, serrno=%d\n",\
        (Z),rc,severity,save_errno,save_serrno); \
        if ( mode == WRITE_DISABLE && \
          (rc == -1 || (severity & (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV)) != 0) && \
          (rtcpd_CheckProcError() & (RTCP_FAILED|RTCP_RESELECT_SERV)) == 0 ) { \
            (void)rtcpd_WaitCompletion(tape,file); \
            if ( (severity & (RTCP_FAILED | RTCP_RESELECT_SERV)) != 0 ) \
                rtcpd_SetProcError(severity); \
            else if ( (severity & RTCP_LOCAL_RETRY) == 0 ) \
                rtcpd_SetProcError(RTCP_FAILED); \
            if ( rtcpd_stageupdc(tape,file) == -1 ) { \
                rtcpd_CheckReqStatus((X),(Y),NULL,&severity); \
                rtcpd_SetProcError(severity); \
            } \
            if ( (severity & RTCP_LOCAL_RETRY) == 0 ) { \
                rtcp_log(LOG_DEBUG,"diskIOthread() return RC=-1 to client\n"); \
                if ( rc == 0 && AbortFlag != 0 && (severity & (RTCP_FAILED|RTCP_RESELECT_SERV)) == 0 ) \
                    rtcpd_SetReqStatus(X,Y,(AbortFlag == 1 ? ERTUSINTR : ERTOPINTR),rtcpd_CheckProcError()); \
                (void) tellClient(&client_socket,X,Y,-1); \
            } else { \
                rtcp_log(LOG_DEBUG,"diskIOthread() return RC=0 to client\n"); \
                (void) tellClient(&client_socket,X,Y,0); \
            } \
            if ( AbortFlag == 0 ) \
                (void)rtcp_WriteAccountRecord(client,tape,file,RTCPEMSG); \
        } \
        if ( disk_fd != -1 ) \
            (void)DiskFileClose(disk_fd,pool_index,tape,file); \
        if ( (severity & RTCP_LOCAL_RETRY) != 0 && mode == WRITE_DISABLE ) \
            rtcpd_SetProcError(severity); \
        if ( AbortFlag == 0 ) rtcpd_BroadcastException(); \
        DiskIOfinished(); \
        if ( rc == -1 ) return((void *)&failure); \
        else return((void *)&success); \
    }}

void *diskIOthread(void *arg) {
    tape_list_t *tape, *tl;
    file_list_t *file, *fl;
    rtcpClientInfo_t *client;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    diskIOstatus_t *diskIOstatus = NULL;
    u_signed64 nbbytes;
    SOCKET client_socket;
    char *p, u64buf[22];
    register int debug = Debug;
    int pool_index = -1;
    int indxp = 0;
    int offset = 0;
    int disk_fd = -1;
    int last_file = FALSE;
    int end_of_tpfile = FALSE;
    int rc, save_rc, mode, severity, save_errno,save_serrno;
    extern char *u64tostr _PROTO((u_signed64, char *, int));
    extern int ENOSPC_occurred;

    rtcp_log(LOG_DEBUG,"diskIOthread() started\n");
    if ( arg == NULL ) {
        rtcp_log(LOG_ERR,"diskIOthread() received NULL argument\n");
        rtcpd_SetProcError(RTCP_FAILED);
        DiskIOfinished();
        return((void *)&failure);
    }

    tape = ((thread_arg_t *)arg)->tape;
    file = ((thread_arg_t *)arg)->file;
    client = ((thread_arg_t *)arg)->client;
    client_socket = ((thread_arg_t *)arg)->client_socket;
    pool_index = ((thread_arg_t *)arg)->pool_index;
    indxp = ((thread_arg_t *)arg)->start_indxp;
    offset = ((thread_arg_t *)arg)->start_offset;
    last_file = ((thread_arg_t *)arg)->last_file;
    end_of_tpfile = ((thread_arg_t *)arg)->end_of_tpfile;

    if ( file == NULL || tape == NULL ) {
        rtcp_log(LOG_ERR,"diskIOthread() received NULL tape/file element\n");
        rtcpd_SetProcError(RTCP_FAILED);
        DiskIOfinished();
        return((void *)&failure);
    }
    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    tapereq = &tape->tapereq;
    filereq = &file->filereq;
    mode = tapereq->mode;

    /*
     * Open the disk file given the specified record format (U/F).
     */
    rc = 0;
    disk_fd = -1;
    severity = RTCP_OK;
    /*
     * For a stagein request (tape read from a stager) we must wait
     * for the stageupdc after the tape position (in tapeIOthread)
     * since the stager may have selected a new path.
     * We have to be careful with setting processing error. If we
     * are writing to tape the tape IO is behind us and it must be
     * able to finish with previous files before detecting the error.
     * On tape read, we are behind the tape IO so we can safely 
     * interrupt the whole processing if there was an error.
     */
    DK_STATUS(RTCP_PS_WAITMTX);
    rc = rtcpd_WaitForPosition(tape, file);
    DK_STATUS(RTCP_PS_NOBLOCKING);
    CHECK_PROC_ERR(file->tape,file,"rtcpd_WaitForPosition() error");

    /*
     * EOD on read is processed later (not always an error).
     */
    if ( (mode == WRITE_ENABLE) || ((severity & RTCP_EOD) == 0) ) {
        if ( mode == WRITE_DISABLE ) {
            DK_STATUS(RTCP_PS_STAGEUPDC);
            rc = rtcpd_stageupdc(tape,file);
            DK_STATUS(RTCP_PS_NOBLOCKING);
            CHECK_PROC_ERR(NULL,file,"rtcpd_stageupdc() error");
            if ( ENOSPC_occurred == TRUE ) {
                rtcp_log(LOG_INFO,"diskIOthread() exit for synchronization due to ENOSPC\n");
                filereq->proc_status = RTCP_WAITING;
                DiskIOfinished();
                return((void *)&success);
            }
        }

        rc = DiskFileOpen(pool_index,tape,file);
        disk_fd = rc;
        rtcpd_CheckReqStatus(file->tape,file,NULL,&severity);
        /*
         * EOD on read is processed later (not always an error).
         * Note: DiskFileOpen() cannot return non-error status in case of
         * a RTCP_EOD severity. This can happen if the open is blocked
         * because of synchronisation for append. Thus, we must check
         * if RTCP_EOD severity before checking the return status to assure
         * proper EOD processing (otherwise it will always be considered as an
         * error).
         */
        if ( (mode == WRITE_ENABLE) || ((severity & RTCP_EOD) == 0) ) {
            CHECK_PROC_ERR(file->tape,file,"DiskFileOpen() error");

            /*
             * Note that MemoryToDisk() and DiskToMemory() close the
             * disk file descriptor if the transfer was successful.
             */
            if ( mode == WRITE_DISABLE ) {
                rc = MemoryToDisk(disk_fd,pool_index,&indxp,&offset,
                                  &last_file,&end_of_tpfile,tape,file);
                if ( rc == 0 ) disk_fd = -1;
                CHECK_PROC_ERR(file->tape,file,"MemoryToDisk() error");
            } else {
                rc = DiskToMemory(disk_fd,pool_index,&indxp,&offset,
                                  &last_file,&end_of_tpfile,tape,file);
                if ( rc == 0 ) disk_fd = -1;
                CHECK_PROC_ERR(file->tape,file,"DiskToMemory() error");
            }
        }
    }

    save_rc = rc;

    if ( mode == WRITE_ENABLE && (filereq->concat & VOLUME_SPANNING) != 0 ) {
        /*
         * If disk files are concatenated into a single tape file that
         * spanns several volumes we should update the disk IO
         * specific data for all this disk fseq on all volumes.
         */
        CLIST_ITERATE_BEGIN(tape,tl) {
            CLIST_ITERATE_BEGIN(tl->file,fl) {
                if ( tl != tape && fl != file && 
                     (fl->filereq.concat & VOLUME_SPANNING) != 0 &&
                     fl->filereq.disk_fseq == filereq->disk_fseq &&
                     strcmp(fl->filereq.file_path,filereq->file_path) == 0 ) {
                    fl->filereq.TEndTransferDisk = filereq->TEndTransferDisk;
                    fl->filereq.TStartTransferDisk = filereq->TStartTransferDisk;
                    strcpy(fl->filereq.ifce,filereq->ifce);
                }
             } CLIST_ITERATE_END(tl->file,fl);
        } CLIST_ITERATE_END(tape,tl);
    }

    /*
     * Update request status if we are behind tape IO (i.e. tape read).
     */
    if ( mode == WRITE_DISABLE ) {
        nbbytes = file->diskbytes_sofar;
        if ( (filereq->concat & CONCAT) != 0 ) nbbytes -= filereq->startsize;
        if ( (filereq->concat & CONCAT_TO_EOD) != 0 ) {
            if ( (severity & RTCP_EOD) == 0 ) {
                nbbytes -= filereq->startsize;
                filereq->proc_status = RTCP_PARTIALLY_FINISHED;
            } else {
                filereq->proc_status = RTCP_FINISHED;
            }
        } else if ( (filereq->concat & VOLUME_SPANNING) != 0 ) { 
            nbbytes = 0;
            CLIST_ITERATE_BEGIN(tape,tl) {
                CLIST_ITERATE_BEGIN(tl->file,fl) {
                    if ( (fl->filereq.concat & VOLUME_SPANNING) != 0 &&
                         fl->filereq.disk_fseq == filereq->disk_fseq &&
                        strcmp(fl->filereq.file_path,filereq->file_path) == 0 &&
                         fl->filereq.proc_status < RTCP_PARTIALLY_FINISHED ) {
                        nbbytes += fl->diskbytes_sofar;
                        /*
                         * The disk transfer time per file section is not
                         * measured since there is only one disk IO thread
                         * for the whole file. The best we can do is to
                         * tape transfer times. 
                         */
                        fl->filereq.TStartTransferDisk = fl->filereq.TStartTransferTape; 
                        fl->filereq.TEndTransferDisk = fl->filereq.TEndTransferTape;
                        strcpy(fl->filereq.ifce,filereq->ifce);
                        fl->filereq.cprc = filereq->cprc;
                        if ( tl->next == tape && fl->next == tl->file ) {
                            fl->filereq.proc_status = RTCP_FINISHED;
                            file = fl; 
                            filereq = &fl->filereq;
                        } else {
                            fl->filereq.proc_status = RTCP_EOV_HIT;
                            rc = tellClient(&client_socket,NULL,fl,save_rc);
                            CHECK_PROC_ERR(NULL,fl,"tellClient() error");
                        }
                    }
                } CLIST_ITERATE_END(tl->file,fl);
            } CLIST_ITERATE_END(tape,tl);
        } else filereq->proc_status = RTCP_FINISHED;
        p = u64tostr(nbbytes,u64buf,0);
        p = strchr(u64buf,' ');
        if ( p != NULL ) p = '\0';
        if ( nbbytes > 0 ) 
            rtcp_log(LOG_INFO,
                     "network interface for data transfer (%s bytes) is %s\n",
                     u64buf,filereq->ifce);

        fl = NULL;
        if ( (filereq->convert & FIXVAR) != 0 ) {
            /*
             * If client asked for record blocking the disk file size is
             * different from the size transferred through the memory buffers
             * because data is converted in a temporary memory buffer before
             * being written to disk. The true disk file size must be reported
             * back to the client but unfortunately we cannot use bytes_out
             * in the current filereq for that: it may still be needed by the
             * start disk IO routine for calculation of the next file boundary
             * in the memory buffers. Thus, we create a temporary element with
             * bytes_out corrected to actual disk file size.
             */
            fl = (file_list_t *)malloc(sizeof(file_list_t));
            if ( fl == NULL ) fl = file;
            else *fl = *file;
            file = fl;
            filereq = &file->filereq;
            filereq->bytes_out = file->diskbytes_sofar - filereq->startsize;
        }

        rtcp_log(LOG_DEBUG,
           "diskIOthread() send %d status for FSEQ %d, FSEC %d on volume %s\n",
           filereq->proc_status,filereq->tape_fseq,
           file->tape_fsec,tapereq->vid);

        rc = tellClient(&client_socket,NULL,file,save_rc);
        CHECK_PROC_ERR(NULL,file,"tellClient() error");
        /*
         * If we are reading from tape, we must tell the
         * stager that the data has successfully arrived at
         * its final destination. Note, for tape write it is
         * the tape IO thread who will update the stager.
         */
        DK_STATUS(RTCP_PS_STAGEUPDC);
        rc = rtcpd_stageupdc(tape,file);
        DK_STATUS(RTCP_PS_NOBLOCKING);
        /*
         * Make sure that stageupdc errors are handled. Note that
         * processing errors from other threads does not need to be
         * handled here since we're going to exit anyway. Besides,
         * handling them will cause a duplicate stage_updc_filcp()
         * which in turn can be pretty bad for the stager.
         */
        if ( rc == -1 ) CHECK_PROC_ERR(NULL,file,"rtcpd_stageupdc() error");

        (void)rtcp_WriteAccountRecord(client,tape,file,RTCPPRC); 

        rtcp_log(LOG_DEBUG,"diskIOthread() fseq %d <-> %s copied %d bytes, rc=%d, proc_status=%d severity=%d\n",
            filereq->tape_fseq,filereq->file_path,
            (unsigned long)file->diskbytes_sofar,filereq->cprc,
            filereq->proc_status,severity);
        if ( (filereq->convert & FIXVAR) != 0 && fl != NULL ) free(fl);
    } /* if ( mode == WRITE_DISABLE ) */

    DiskIOfinished();
    return((void *)&success);
}


int rtcpd_InitDiskIO(int *poolsize) {
    extern char *getenv();
    extern char *getconfent();
    char *p;
    int rc;
    
    if ( poolsize == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( (p = getenv("RTCPD_THREAD_POOL")) != (char *)NULL ) {
        *poolsize = atoi(p);
    } else if ( ( p = getconfent("RTCPD","THREAD_POOL",0)) != (char *)NULL ) {
        *poolsize = atoi(p);
    } else {
#if defined(RTCPD_THREAD_POOL)
        *poolsize = RTCPD_THREAD_POOL;
#else /* VDQM_THREAD_POOL */
        *poolsize = 3;         /* Set some reasonable default */
#endif /* VDQM_TRHEAD_POOL */
    }
    
    rc = Cpool_create(*poolsize,poolsize);
    rtcp_log(LOG_DEBUG,"rtcpd_InitDiskIO() thread pool (id=%d): pool size = %d\n",
        rc,*poolsize);
    /*
     * Create the diskIOstatus array in the processing status structure
     */
    proc_stat.diskIOstatus = (diskIOstatus_t *)calloc(*poolsize,sizeof(diskIOstatus_t));
    if ( proc_stat.diskIOstatus == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_InitDiskIO() calloc(%d,%d): %s\n",
            *poolsize,sizeof(diskIOstatus_t),sstrerror(errno));
        rc = -1;
    }
    proc_stat.nb_diskIO = *poolsize;

    return(rc);
}

int rtcpd_CleanUpDiskIO(int poolID, int poolsize) {
    int thIndex;
    for ( thIndex = 0; thIndex < poolsize; thIndex++ ) {
        tellClient(&thargs[thIndex].client_socket,NULL,NULL,0);
        rtcp_CloseConnection(&thargs[thIndex].client_socket);
    }
    if ( thargs != NULL ) free(thargs);
    return(0);
}

int rtcpd_StartDiskIO(rtcpClientInfo_t *client,
                      tape_list_t *tape,
                      file_list_t *file,
                      int poolID, int poolsize) {

    tape_list_t *nexttape;
    file_list_t *nextfile, *prevfile;
    u_signed64 prev_filesz;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    diskIOstatus_t *diskIOstatus;
    thread_arg_t *tharg;
    int rc, save_serrno, indxp, offset, next_offset, last_file,end_of_tpfile, spill;
    int prev_bufsz, next_nb_bufs, severity, next_bufsz, thIndex, mode;
    register int Uformat;
    register int convert;
    register int debug = Debug;

    if ( client == NULL || tape == NULL || file == NULL ||
        poolsize <= 0 ) {
        serrno = EINVAL;
        return(-1);
    }

    rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO() called\n");
    /*
     * Reserve a thread argument table
     */
    if ( thargs == NULL ) {
        thargs = (thread_arg_t *)malloc(poolsize * sizeof(thread_arg_t));
        if ( thargs == NULL ) {
            save_serrno = errno;
            rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() malloc(): %s\n",
                sstrerror(errno));
            serrno = save_serrno;
            return(-1);
        }
        for ( thIndex = 0; thIndex < poolsize; thIndex++ ) {
            /*
             * Open a separate connection to client. An unique
             * connection for each thread allows them to talk
             * independently to the client. The connections are
             * closed at cleanup after the full request has finished
             * (in rtcpd_CleanUpDiskIO()).
             */
            tharg = &thargs[thIndex];
            tharg->client_socket = INVALID_SOCKET;
            rc = rtcpd_ConnectToClient(&tharg->client_socket,
                                       client->clienthost,
                                       &client->clientport);
            if ( rc == -1 ) {
                save_serrno = serrno;
                rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() rtcpd_ConnectToClient(%s,%d): %s\n",
                    client->clienthost,client->clientport,sstrerror(serrno));
                serrno = save_serrno;
                return(-1);
            }
        }
    }

    /*
     * Loop over all file requests
     */
    indxp = offset = next_offset = 0;
    last_file = FALSE;
    prevfile = NULL;
    /*
     * We can safely set diskIOstarted flag here. It is only used
     * by us and the diskIOthread's we will start in the following.
     * It must be set to one to allow the first diskIOthread to
     * start.
     */
    proc_cntl.diskIOstarted = 1;
    proc_cntl.diskIOfinished = 0;
    proc_cntl.nb_diskIOactive = 0;
    proc_cntl.nb_reserved_bufs = 0;
    nexttape = tape;
    mode = tape->tapereq.mode;
    /*
     * We don't loop over volumes since volume spanning is only allowed
     * for last tape file in the request and the file must start in the 
     * first volume.
     */
    tapereq = &nexttape->tapereq;
    CLIST_ITERATE_BEGIN(nexttape->file,nextfile) {
        end_of_tpfile = FALSE;
        thIndex = -1;

        /*
         * Leading I/O thread must always check if clients wishes
         * to append more work. Trailing I/O thread must always
         * wait for leading I/O thread to do this check in case
         * they have reached the same file request.
         */
        if ( mode == WRITE_ENABLE ) {
            /*
             * Grab a free socket
             */
            thIndex = Cpool_next_index(poolID);
            if ( thIndex == -1 ) {
                save_serrno = serrno;
                rtcpd_AppendClientMsg(NULL, nextfile, "Error assigning thread: %s\n",
                    sstrerror(save_serrno));
                rtcpd_SetReqStatus(NULL,nextfile,save_serrno,
                                   RTCP_SYERR | RTCP_RESELECT_SERV);

                (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
                proc_cntl.diskIOfinished = 1;
                (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cpool_next_index(): %s\n",
                         sstrerror(save_serrno));
                serrno = save_serrno;
                return(-1);
            }
            rtcp_log(LOG_INFO,"rtcpd_StartDiskIO() check with client for more work. Use socket %d\n",
                thargs[thIndex].client_socket);
            rc = rtcpd_checkMoreWork(&(thargs[thIndex].client_socket),
                nexttape,nextfile);
            if ( rc == -1 ) {
                save_serrno = serrno;
                rtcpd_AppendClientMsg(NULL, nextfile, "Error requesting client for more work: %s\n",
                    sstrerror(save_serrno));
                rtcpd_SetReqStatus(NULL,nextfile,save_serrno,
                                   RTCP_USERR | RTCP_FAILED);
                (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
                proc_cntl.diskIOfinished = 1;
                (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() rtcpd_checkMoreWork(): %s\n",
                         sstrerror(save_serrno));
                serrno = save_serrno;
                return(-1);
            }
            if ( rc == 1 ) break;
        } else {
            rtcp_log(LOG_INFO,"rtcpd_StartDiskIO() end of filereqs. Wait for tapeIO to check for more work\n");
            rc = rtcpd_waitMoreWork(nextfile);
            if ( rc == -1 ) {
                save_serrno = serrno;
                rtcpd_AppendClientMsg(NULL, nextfile, "Error waiting for client to request more work: %s\n",
                    sstrerror(save_serrno));
                rtcpd_SetReqStatus(NULL,nextfile,save_serrno,
                                   RTCP_USERR | RTCP_FAILED);
                (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
                proc_cntl.diskIOfinished = 1;
                (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() rtcpd_waitMoreWork(): %s\n",
                         sstrerror(save_serrno));
                serrno = save_serrno;
                return(-1);
            }
            /*
             * Break out if there is nothing more to do
             */
            if ( rc == 1 ) break;
        }

        filereq = &nextfile->filereq;
        Uformat = (*filereq->recfm == 'U' ? TRUE : FALSE);
        convert = filereq->convert;
        if ( nextfile->next == nexttape->file ) last_file = TRUE;
        if ( (last_file == TRUE) || (nextfile->next->filereq.concat & 
             (NOCONCAT | NOCONCAT_TO_EOD)) != 0 ) end_of_tpfile = TRUE;
        if ( nextfile->filereq.proc_status != RTCP_FINISHED ) {
            /*
             * Get control info
             */
            rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
            if ( rc == -1 ) {
                save_serrno = serrno;
                rtcpd_AppendClientMsg(NULL, nextfile, "Cannot lock mutex: %s\n",
                    sstrerror(save_serrno));
                rtcpd_SetReqStatus(NULL,nextfile,save_serrno,
                                   RTCP_RESELECT_SERV | RTCP_SYERR);
                rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cthread_mutex_lock_ext(): %s\n",
                    sstrerror(save_serrno));
                serrno = save_serrno;
                return(-1);
            }
            if ( mode == WRITE_ENABLE ) 
                filereq->err.severity = filereq->err.severity & ~RTCP_LOCAL_RETRY;

            /*
             * Check if we need to exit due to processing error
             */
            rc= rtcpd_CheckProcError();
            if ( rc != RTCP_OK ) {
                rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() processing error detected, severity=0x%x (%d)\n",rc,rc);
                proc_cntl.diskIOfinished = 1;
                (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                /* It's not our error */
                return(0);
            }
            /*
             * Did the tape end with previous file ?
             */
            if ( mode == WRITE_DISABLE && prevfile != NULL &&
                 (prevfile->filereq.concat & (CONCAT_TO_EOD|NOCONCAT_TO_EOD)) !=0 ) {
                rtcpd_CheckReqStatus(prevfile->tape,prevfile,NULL,&severity);
                if ( (severity & RTCP_EOD) != 0 ) {
                    proc_cntl.diskIOfinished = 1;
                    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                    return(0);
                }
            }

            /*
             * Wait while buffers are overallocated. We also
             * have to make sure that the previously dispatched thread
             * started and locked its first buffer. For tape write
             * there are one additional conditions:
             * 1) we must check if we need to wait for the 
             *    Ctape_info() for more info. (i.e. blocksize)
             *    before starting the disk -> memory copy.
             * For tape read we must also check if the tapeIO has
             * finished, in which case we can just go on copying the
             * the remainin data in memory to disk.
             */
            rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO(): nb_reserver_bufs=%d, nb_bufs=%d, mode=%d, diskIOstarted=%d, tapeIOfinished=%d, blocksize=%d\n",
                    proc_cntl.nb_reserved_bufs,nb_bufs,mode,
                    proc_cntl.diskIOstarted,proc_cntl.tapeIOfinished,
                    filereq->blocksize);
            while ( (proc_cntl.diskIOstarted == 0) ||
                    (proc_cntl.nb_reserved_bufs >= nb_bufs &&
                     (mode == WRITE_ENABLE || 
                      (mode == WRITE_DISABLE &&
                       proc_cntl.tapeIOfinished == 0 &&
                       prevfile != NULL &&
                       prevfile->end_index < 0))) ||
                    ((mode == WRITE_ENABLE) &&
                     (filereq->blocksize <= 0) &&
                     ((filereq->concat & NOCONCAT) != 0)) ) {
                rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO() waiting... (nb_reserved_bufs=%d, diskIOstarted=%d\n",
                         proc_cntl.nb_reserved_bufs,proc_cntl.diskIOstarted);
                rc = Cthread_cond_wait_ext(proc_cntl.cntl_lock);
                if ( rc == -1 ) {
                    save_serrno = serrno;
                    rtcpd_AppendClientMsg(NULL, nextfile, "Error on condition wait: %s\n",
                        sstrerror(save_serrno));
                    rtcpd_SetReqStatus(NULL,nextfile,save_serrno,
                                      RTCP_SYERR | RTCP_RESELECT_SERV);
                    rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cthread_cond_wait_ext(proc_cntl): %s\n",
                        sstrerror(save_serrno));
                    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                    serrno = save_serrno;
                    return(-1);
                }
                /*
                 * Check if we need to exit due to processing error
                 */
                rc= rtcpd_CheckProcError();
                if ( rc != RTCP_OK ) {
                    rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() processing error detected, severity=0x%x (%d)\n",rc,rc);
                    proc_cntl.diskIOfinished = 1;
                    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                    /* It's not our error */
                    return(0);
                }
                /*
                 * Did the tape end with previous file ?
                 */
                if ( mode == WRITE_DISABLE && prevfile != NULL &&
                     (prevfile->filereq.concat & (CONCAT_TO_EOD|NOCONCAT_TO_EOD)) !=0 ) {          
                    rtcpd_CheckReqStatus(prevfile->tape,prevfile,NULL,&severity);
                    if ( (severity & RTCP_EOD) != 0 ) {
                        proc_cntl.diskIOfinished = 1;
                        (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                        (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                        return(0);
                    }
                }
            }

            /*
             * We're going to start a new diskIO thread. 
             */
            proc_cntl.diskIOstarted = 0;
            proc_cntl.nb_diskIOactive++;
            /*
             * Calculate nb buffers to be used for the next file.
             * Don't touch this code unless you REALLY know what you are
             * doing. Any mistake here may in the best case result in deadlocks
             * and in the worst case .... data corruption!
             */
            if ( mode == WRITE_ENABLE ) {
                /*
                 * On tape write we know the next file size
                 */
                next_bufsz = rtcpd_CalcBufSz(nexttape,nextfile);
                if ( (filereq->concat & CONCAT) != 0 ) 
                    next_nb_bufs = (int)(((u_signed64)next_offset + 
                          nextfile->filereq.bytes_in) / (u_signed64)next_bufsz);
                else 
                    next_nb_bufs = (int)(nextfile->filereq.bytes_in / 
                                   (u_signed64)next_bufsz);
                /*
                 * Increment by one if not concatenating or we reached last
                 * file in the concatenation (subsequent file will start
                 * with a new buffer).
                 */
                if ( (nextfile->next->filereq.concat & NOCONCAT) != 0 ) 
                    next_nb_bufs++;
            } else {
                /*
                 * On tape read we don't know the file size. Make
                 * sure to overallocate until we know the exact size
                 * set by the tape IO thread at end of file.
                 */
                next_nb_bufs = nb_bufs + 1;
            }
            rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO() number of reserved buffs %d + %d\n",
                proc_cntl.nb_reserved_bufs,next_nb_bufs);

            proc_cntl.nb_reserved_bufs += next_nb_bufs;
            rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO() new number of reserved buffs %d\n",
                proc_cntl.nb_reserved_bufs);

            rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
            if ( rc == -1 ) {
                save_serrno = serrno;
                rtcpd_AppendClientMsg(NULL,nextfile,
                      "Cannot unlock CNTL mutex: %s\n",sstrerror(serrno));
                rtcpd_SetReqStatus(NULL,nextfile,serrno,
                                   RTCP_SYERR | RTCP_RESELECT_SERV);
                rtcp_log(LOG_ERR,
                         "rtcpd_StartDiskIO() Cthread_mutex_unlock_ext(): %s\n",
                         sstrerror(serrno));
                serrno = save_serrno;
                return(-1);
            }

            /* 
             * Update offset in buffer table.
             * Don't touch this code unless you REALLY know what you are
             * doing. Any mistake here may in the best case result in deadlocks
             * and in the worst case .... data corruption!
             */
            if ( prevfile == NULL ) {
                /*
                 * First file
                 */
                indxp = offset = next_offset = 0;
            } else {
                prev_bufsz = rtcpd_CalcBufSz(nexttape,prevfile);
                if ( mode == WRITE_ENABLE ) 
                    prev_filesz = prevfile->filereq.bytes_in;
                else 
                    prev_filesz = prevfile->filereq.bytes_out;
                rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO() prev. file size %d, buffer sz %d, indxp %d\n",
                        (int)prev_filesz,prev_bufsz,indxp);
                if ( mode == WRITE_ENABLE ) 
                    indxp = (indxp + (int)(((u_signed64)offset + prev_filesz) /
                            ((u_signed64)prev_bufsz)));
                else
                    indxp = prevfile->end_index;
                rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO() new indxp %d\n",
                        indxp);

                indxp = indxp % nb_bufs;
                if ( mode == WRITE_ENABLE ) {
                    if ( (filereq->concat & NOCONCAT) != 0 ||
                       ((Uformat == TRUE) && ((convert & NOF77CW) == 0)) ) {
                        /*
                         * Not concatenating on tape. Start next file
                         * with a brand new buffer except if previous
                         * file was empty we re-use the previous buffer.
                         * Also true U-format files start with new buffer
                         * since in this case concatenation does 
                         * not imply any problems with partial blocks.
                         */
                        rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO(): no concatenate (%d != %d), indxp %d\n",
                            nextfile->filereq.tape_fseq,prevfile->filereq.tape_fseq,indxp);

                        if ( prev_filesz != 0 ) 
                            indxp = (indxp + 1) % nb_bufs;
                        offset = next_offset = 0;
                        /*
                         * If next file is concatenated we must provide
                         * correct offset so that the next_nb_bufs calculation
                         * in next iteration is correct.
                         */
                        if ( (nextfile->next->filereq.concat & CONCAT) != 0 &&
                             (*nextfile->next->filereq.recfm != 'U' ||
                              (nextfile->next->filereq.convert & NOF77CW) != 0) )
                            next_offset = (int)(filereq->bytes_in/((u_signed64)next_bufsz));
                    } else {
                        /*
                         * On tape write we need offset if we are
                         * concatenating on tape
                         */
                        rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO(): concatenate (%d == %d)\n",
                            nextfile->filereq.tape_fseq,prevfile->filereq.tape_fseq);
                        offset = (int)(((u_signed64)offset + prev_filesz) %
                                       ((u_signed64)prev_bufsz));
                        next_offset = (int)(((u_signed64)offset + filereq->bytes_in) %
                                            ((u_signed64)next_bufsz));
                    }
                }
            }
            prevfile = nextfile;
            /*
             * Get next thread index and fill in thread args
             */
            if ( thIndex < 0 ) thIndex = Cpool_next_index(poolID);
            if ( thIndex == -1 ) {
                save_serrno = serrno;
                rtcpd_AppendClientMsg(NULL, nextfile, "Error assigning thread: %s\n",
                    sstrerror(save_serrno));
                rtcpd_SetReqStatus(NULL,nextfile,save_serrno,
                                   RTCP_SYERR | RTCP_RESELECT_SERV);
                rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cpool_next_index(): %s\n",
                         sstrerror(save_serrno));
                serrno = save_serrno;
                return(-1);
            }
            tharg = &thargs[thIndex];

            tharg->tape = nexttape;
            tharg->file = nextfile;
            tharg->client = client;
            tharg->end_of_tpfile = end_of_tpfile;
            tharg->last_file = last_file;
            tharg->pool_index = thIndex;
            tharg->start_indxp = indxp;
            tharg->start_offset = offset;
            /*
             * Reset the diskIOstatus for assigned thread
             */
            diskIOstatus = &proc_stat.diskIOstatus[thIndex];
            DK_STATUS(RTCP_PS_NOBLOCKING);
            DK_SIZE(0);
            /*
             * Assign next thread and start the request
             */
            rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO(thIndex=%d,arg=0x%lx) start with indxp=%d, offset=%d, end_of_tpfile=%d\n",
                thIndex,tharg,indxp,offset,end_of_tpfile);
            rc = Cpool_assign(poolID,diskIOthread,(void *)tharg,-1);
            if ( rc == -1 ) {
                save_serrno = serrno;
                rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cpool_assign(%d): %s\n",
                         poolID,sstrerror(serrno));
                serrno = save_serrno;
                return(-1);
            }
            /*
             * Have we reached End Of Data on tape in a CONCAT_TO_EOD or
             * NOCONCAT_TO_EOD request ?
             */
            if ( mode == WRITE_DISABLE &&
                (filereq->err.severity & RTCP_EOD) != 0 ) break;

        } else { /* if ( ... ) */
            rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO() skipping finished request (%d,%d,%s,concat:%d\n",
                     nextfile->filereq.tape_fseq,nextfile->filereq.disk_fseq,
                     nextfile->filereq.file_path,nextfile->filereq.concat);
        }
    } CLIST_ITERATE_END(nexttape->file,nextfile);
    /*
     * Tell the others that we've finished
     */
    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cthread_mutex_lock_ext(): %s\n",
                 sstrerror(serrno));
        serrno = save_serrno;
        return(-1);
    }
    proc_cntl.diskIOfinished = 1;
    rc = Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cthread_cond_broadcast_ext(): %s\n",
                 sstrerror(serrno));
        serrno = save_serrno;
        return(-1);
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cthread_mutex_unlock_ext(): %s\n",
                 sstrerror(serrno));
        serrno = save_serrno;
        return(-1);
    }

    return(0);
}

