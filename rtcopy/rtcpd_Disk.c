/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_Disk.c,v $ $Revision: 1.9 $ $Date: 1999/12/15 07:31:22 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_MainCntl.c - RTCOPY server main control thread
 */

#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <time.h>
#include <io.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
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
#define RFIO_KERNEL 1
#include <rfio.h>
#include <rfio_errno.h>
#include <Cthread_api.h>
#include <Cpool_api.h>
#include <vdqm_api.h>
#include <Ctape_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <serrno.h>

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
} thread_arg_t;

extern int Debug;

extern int nb_bufs;

extern int bufsz;

extern processing_cntl_t proc_cntl;

extern processing_status_t proc_stat;

extern buffer_table_t **databufs;

extern rtcpClientInfo_t *client;

static int FortranUnitTable[NB_FORTRAN_UNITS];  /* Table of Fortran units */

int success = 0;
int failure = -1;

thread_arg_t *thargs;

static int DiskIOstarted() {
    int rc;
    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"DiskIOstarted(): Cthread_mutex_lock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        rtcpd_SetProcError(RTCP_FAILED);
        return(-1);
    }
    proc_cntl.diskIOstarted = 1;
    rc = Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"DiskIOstarted(): Cthread_cond_broadcast_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        rtcpd_SetProcError(RTCP_FAILED);
        return(-1);
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"DiskIOstarted(): Cthread_mutex_unlock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        rtcpd_SetProcError(RTCP_FAILED);
        return(-1);
    }
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
        rtcpd_SetProcError(RTCP_FAILED);
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
        rtcpd_SetProcError(RTCP_FAILED);
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
        rtcpd_SetProcError(RTCP_FAILED);
        return(-1);
    }
    FortranUnitTable[file->FortranUnit] = FALSE;
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"ReturnFortranUnit(): Cthread_mutex_unlock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        rtcpd_SetProcError(RTCP_FAILED);
        return(-1);
    }
    file->FortranUnit = -1;
    return(0);
}
    

static int DiskFileOpen(int pool_index, 
                        tape_list_t *tape,
                        file_list_t *file) {
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    int rc, irc, save_serrno, disk_fd, flags, use_rfioV3;
    diskIOstatus_t *diskIOstatus = NULL;
    char *ifce;
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

    /*
     * Open the disk file
     */
    if ( *filereq->recfm == 'F' ) {
        /*
         * Normal Formatted file option. 
         * Open the disk file
         */
        flags = O_RDONLY | binmode;
        if ( tapereq->mode == WRITE_DISABLE ) {
            if ( (filereq->concat & (CONCAT | CONCAT_TO_EOD)) ) {
                /*
                 * Appending to previous disk file. 
                 */
                flags = O_WRONLY | O_APPEND | binmode;
            } else {
                /*
                 * New disk file
                 */
                flags = O_CREAT | O_WRONLY | O_TRUNC | binmode;
            }
        }
        /* Activate new transfer mode for source file */
        use_rfioV3 = RFIO_STREAM;
        rfiosetopt(RFIO_READOPT,&use_rfioV3,4); 
        
        serrno = 0;
        rfio_errno = 0;
        DEBUG_PRINT((LOG_DEBUG,"DiskFileOpen() open(%s,0x%x\n",filereq->file_path,
            flags));
        DK_STATUS(RTCP_PS_OPEN);
        rc = rfio_open(filereq->file_path,flags,0666);
        DK_STATUS(RTCP_PS_NOBLOCKING);
        if ( rc == -1 ) {
            save_serrno = rfio_errno;
            rtcp_log(LOG_ERR,
                "DiskFileOpen() rfio_open(%s,0x%x): errno = %d, serrno = %d, rfio_errno = %d\n",
                filereq->file_path,flags,errno,serrno,rfio_errno);
        } else {
            disk_fd = rc;
            rc = 0;
        }
        DEBUG_PRINT((LOG_DEBUG,"DiskFileOpen() rfio_open() returned fd=%d\n",
            disk_fd));
    } else if ( *filereq->recfm == 'U' ) {
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
            if ( (filereq->concat & (CONCAT | CONCAT_TO_EOD)) ) {
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
        DK_STATUS(RTCP_PS_OPEN);
        rc = rfio_xyopen(filereq->file_path," ",file->FortranUnit,0,
                         Uformat_flags,&irc);
        DK_STATUS(RTCP_PS_NOBLOCKING);
        if ( rc != 0 || irc != 0 ) {
            save_serrno = (rc != 0 ? rc : irc);
            rtcp_log(LOG_ERR,
                "DiskFileOpen() rfio_open(%s,%d): errno = %d, serrno = %d, rfio_errno = %d\n",
                filereq->file_path,Uformat_flags,errno,serrno,rfio_errno);
        } else {
            disk_fd = file->FortranUnit;
        }
    } else {
        rtcp_log(LOG_ERR,"DiskFileOpen() unknown recfm %s\n",
            filereq->recfm);
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
        switch (save_serrno) {
        case ENOENT:
        case EISDIR:
        case EPERM:
        case EACCES:
            filereq->err.errorcode = save_serrno;
            rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_USERR | RTCP_FAILED);
            rtcpd_SetProcError(RTCP_USERR | RTCP_FAILED);
            break;
        default:
            rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_UNERR | RTCP_FAILED);
            if (errno = EBADF && rfio_errno == 0 && serrno == 0)
                rtcpd_SetProcError(RTCP_RESELECT_SERV | RTCP_FAILED | RTCP_UNERR);
            else
                rtcpd_SetProcError(RTCP_FAILED | RTCP_UNERR);
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
        if ( *filereq->recfm == 'U' ) s = (SOCKET)rfio_xysock(disk_fd);
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
    int rc, irc;
    diskIOstatus_t *diskIOstatus = NULL;

    if ( file == NULL ) return(-1);
    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    filereq = &file->filereq;

    if ( *filereq->recfm == 'F' ) {
        rc = rfio_close(disk_fd);
    } else if ( *filereq->recfm == 'U' ) {
        rc = rfio_xyclose(file->FortranUnit," ",&irc);
        (void)ReturnFortranUnit(pool_index,file);
    }
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
    int buf_done, nb_bytes;
    register int Uformat;
    register int debug = Debug;
    register int convert;
    char *convert_buffer = NULL;
    char errmsgtxt[80] = {""};
    void *f77conv_context = NULL;
    u_signed64 totsz;
    diskIOstatus_t *diskIOstatus = NULL;
    char *bufp;
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

    /*
     * Main write loop. End with EOF on tape file or error condition
     */
    *end_of_tpfile = FALSE;
    totsz = filereq->startsize;
    DK_SIZE(totsz);
    save_serrno = 0;
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
            DEBUG_PRINT((LOG_DEBUG,"MemoryToDisk() wait on buffer[%d]->flag=%d\n",
                    i,databufs[i]->flag));
            DK_STATUS(RTCP_PS_WAITCOND);
            rc = Cthread_cond_wait_ext(databufs[i]->lock);
            DK_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToDisk() Cthread_cond_wait_ext(): %s\n",
                    sstrerror(serrno));
                if ( convert_buffer != NULL ) free(convert_buffer);
                if ( f77conv_context != NULL ) free(f77conv_context);
                return(-1);
            }
            if ( rtcpd_CheckProcError() & RTCP_FAILED ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                if ( convert_buffer != NULL ) free(convert_buffer);
                if ( f77conv_context != NULL ) free(f77conv_context);
                return(-1);
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
                if ( (lrecl <= 0) && (Uformat == FALSE) ) lrecl = blksiz;
                else if (Uformat == TRUE) lrecl = 0;
                filereq->TStartTransferDisk = (int)time(NULL);
            }
        }
        DEBUG_PRINT((LOG_DEBUG,"MemoryToDisk() buffer %d full\n",i));
        /*
         * Verify that actual buffer size matches block size
         */
        if ( (databufs[i]->length % blksiz) != 0 ) {
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
        if ( databufs[i]->data_length > 0 && save_serrno != ENOSPC ) {
            nb_bytes = databufs[i]->data_length;
            /*
             * Check that the total size does not exceed maxsize specified
             * by user. If so, reduce the nb bytes accordingly.
             */
            if ( (filereq->maxsize > 0) && 
                 (totsz + (u_signed64)nb_bytes > filereq->maxsize) ) {
                nb_bytes = (int)(filereq->maxsize - totsz);
                databufs[i]->data_length = nb_bytes;
                rtcpd_SetReqStatus(NULL,file,EFBIG,RTCP_OK | RTCP_LIMBYSZ);
            }
            if ( nb_bytes > 0 ) {
                if ( (convert & EBCCONV) != 0 ) 
                    ebc2asc(databufs[i]->buffer,nb_bytes);
                /*
                 * >>>>>>>>>>> write to disk <<<<<<<<<<<<<
                 */
                if ( Uformat == FALSE ) {
                    bufp = databufs[i]->buffer;
                    if ( (convert & FIXVAR) != 0 ) {
                        nb_bytes = rtcpd_FixToVar(bufp,
                            convert_buffer,nb_bytes,lrecl);
                        bufp = convert_buffer;
                    } else if ( (convert & F77CONV) != 0 ) {
                        nb_bytes = rtcpd_f77RecToFix(bufp,
                            nb_bytes,lrecl,errmsgtxt,&f77conv_context);
                        if ( *errmsgtxt != '\0' ) 
                            rtcpd_AppendClientMsg(NULL, file,errmsgtxt);
                    }
                    DK_STATUS(RTCP_PS_WRITE);
                    if ( nb_bytes > 0 ) rc = rfio_write(disk_fd,bufp,nb_bytes);
                    else rc = nb_bytes;
                    DK_STATUS(RTCP_PS_NOBLOCKING);
                    if ( rc == -1 ) save_serrno = rfio_errno;
                } else {
                    rc = 0;
                    for (j=0; j*blksiz < nb_bytes; j++) {
                        lrecl = databufs[i]->lrecl_table[j];
                        bufp = databufs[i]->buffer + j*blksiz;
                        DK_STATUS(RTCP_PS_READ);
                        status = rfio_xywrite(file->FortranUnit,bufp,0,
                                         lrecl," ",&irc);
                        DK_STATUS(RTCP_PS_NOBLOCKING);
                        if ( status != 0 || irc != 0 ) {
                            if ( status == ENOSPC || irc == ENOSPC )
                                save_serrno = ENOSPC;
                            else save_serrno = rfio_errno;
                            rc = -1;
                            break;
                        } else rc += lrecl;
                    }
                    lrecl = 0;
                }
                if ( rc != nb_bytes ) {
                    rtcp_log(LOG_ERR,"MemoryToDisk() rfio_write(): %s\n",
                        rfio_serror());
                    /*
                     * In case of ENOSPC we will have to return
                     * to ask the stager for a new path
                     */
                    if ( save_serrno != ENOSPC ) {
                        rtcpd_AppendClientMsg(NULL, file,RT115,"CPTPDSK",    
                            rfio_serror());
                        rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED);
                        (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                        (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                        if ( convert_buffer != NULL ) free(convert_buffer);
                        if ( f77conv_context != NULL ) free(f77conv_context);
                        return(-1);
                    }
                }
            } else {
                /*
                 * Max size exceeded. Just skip over the buffer
                 * to assure that it is marked as free for the
                 * Tape IO thread. We tell the tape IO
                 * thread to stop reading data by setting the
                 * request status to LIMBYSZ.
                 */
                rc = databufs[i]->data_length;
                rtcpd_SetReqStatus(NULL,file,0,RTCP_OK | RTCP_LIMBYSZ);
            }
        } else {
            rc = 0;
        }
        databufs[i]->data_length -= rc;
        totsz += (u_signed64)rc;
        DK_SIZE(totsz);
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
        rc = Cthread_cond_broadcast_ext(databufs[i]->lock);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"MemoryToDisk() Cthread_cond_broadcast_ext(): %s\n",
                sstrerror(serrno));
            if ( convert_buffer != NULL ) free(convert_buffer);
            if ( f77conv_context != NULL ) free(f77conv_context);
            return(-1);
        }
        rc = Cthread_mutex_unlock_ext(databufs[i]->lock);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"MemoryToDisk() Cthreda_mutex_unlock_ext(): %s\n",
                sstrerror(serrno));
            if ( convert_buffer != NULL ) free(convert_buffer);
            if ( f77conv_context != NULL ) free(f77conv_context);
            return(-1);
        }

        if ( *end_of_tpfile == TRUE || save_serrno == ENOSPC ) {
            /*
             * End of tape file reached. Close disk file and tell the
             * main control thread that we exit
             */
            if ( save_serrno == ENOSPC ) {
                (void)DiskFileClose(disk_fd,pool_index,tape,file);
                serrno = save_serrno;
            } else {
                DEBUG_PRINT((LOG_DEBUG,"MemoryToDisk() close disk file fd=%d\n",
                    disk_fd));
                rc = DiskFileClose(disk_fd,pool_index,tape,file);
                save_serrno = rfio_errno;
                if ( rc == -1 ) {
                    rtcp_log(LOG_ERR,"MemoryToDisk() DiskFileClose(%d): %s\n",
                        disk_fd,rfio_serror());
                    /*
                     * In case of ENOSPC we will have to return
                     * to ask the stager for a new path
                     */
                    if ( save_serrno != ENOSPC ) {
                        rtcpd_AppendClientMsg(NULL, file,RT108,"CPTPDSK",
                            rfio_serror());
                        rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED);
                    }
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( f77conv_context != NULL ) free(f77conv_context);
                    return(-1);
                }
            }
            break;
        }
        *indxp = (*indxp + 1) % nb_bufs;
        *offset = 0;

        /*
         * Has something fatal happened while we were occupied
         * reading from the disk?
         */
        if ( rtcpd_CheckProcError() & RTCP_FAILED ) break;
    } /* for (;;) */
    
    if ( convert_buffer != NULL ) free(convert_buffer);
    if ( f77conv_context != NULL ) free(f77conv_context);
    return(0);
}
static int DiskToMemory(int disk_fd, int pool_index,
                        int *indxp, int *offset,
                        int *last_file, int *end_of_tpfile,
                        tape_list_t *tape,
                        file_list_t *file) {
    int rc, irc, i, j, blksiz, lrecl, end_of_dkfile, current_bufsz;
    int status, nb_bytes, SendStartSignal, save_serrno;
    register int Uformat;
    register int debug = Debug;
    u_signed64 totsz;
    diskIOstatus_t *diskIOstatus = NULL;
    char *bufp;
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
    if ( (lrecl <= 0) && (Uformat == FALSE) ) lrecl = blksiz;
    else if ( Uformat == TRUE ) lrecl = 0;
    
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
    totsz = filereq->startsize;
    DK_SIZE(totsz);
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
        while ( databufs[i]->flag == BUFFER_FULL) {
            DK_STATUS(RTCP_PS_WAITCOND);
            rc = Cthread_cond_wait_ext(databufs[i]->lock);
            DK_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"DiskToMemory() Cthread_cond_wait_ext(): %s\n",
                    sstrerror(serrno));
                return(-1);
            }
            if ( rtcpd_CheckProcError() & RTCP_FAILED ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
        }
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
        if ( (filereq->maxsize > 0) &&
             (totsz + (u_signed64)nb_bytes > filereq->maxsize) ) {
            nb_bytes = (int)(filereq->maxsize - totsz);
            end_of_dkfile = TRUE;
            rtcpd_SetReqStatus(NULL,file,EFBIG,RTCP_OK | RTCP_LIMBYSZ);
        }
        /*
         * If U-format, re-allocate the lrecl_table[] if needed.
         */
        if ( Uformat == TRUE ) rc = rtcpd_AdmUformatInfo(file,i);

        /*
         * >>>>>>>>>>> read from disk <<<<<<<<<<<<<
         */
        DEBUG_PRINT((LOG_DEBUG,"DiskToMemory() read %d bytes from %s\n",
            nb_bytes,filereq->file_path));
        if ( Uformat == FALSE ) {
            bufp = databufs[i]->buffer + *offset;
            DK_STATUS(RTCP_PS_READ);
            if ( nb_bytes > 0 ) rc = rfio_read(disk_fd,bufp,nb_bytes);
            else rc = nb_bytes;
            DK_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) save_serrno = rfio_errno;
        } else {
            rc = irc = 0;
            databufs[i]->nbrecs = 0;
            for (j=0; j*blksiz < nb_bytes; j++) {
                bufp = databufs[i]->buffer + j*blksiz;
                DK_STATUS(RTCP_PS_READ);
                status = rfio_xyread(file->FortranUnit,bufp,0,blksiz,
                                     &lrecl,"U",&irc);
                DK_STATUS(RTCP_PS_NOBLOCKING);
                save_serrno = rfio_errno;
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
            rtcp_log(LOG_ERR,"DiskToMemory() rfio_read(): %s\n",
                rfio_serror());
            rtcpd_AppendClientMsg(NULL, file,RT112,"CPDSKTP",
                rfio_serror());
            rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED);
            (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
            (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            return(-1);
        }
        DEBUG_PRINT((LOG_DEBUG,"DiskToMemory() got %d bytes from %s\n",
            rc,filereq->file_path));
        databufs[i]->data_length += rc;
        totsz += (u_signed64)rc;
        DK_SIZE(totsz);

        if ( (end_of_dkfile == TRUE) ||
             ((rc < nb_bytes) && (Uformat == FALSE)) ||
             ((irc == 2) && (Uformat == TRUE)) ) {
            DEBUG_PRINT((LOG_DEBUG,"DiskToMemory() End of file %s reached in buffer %d\n",
                filereq->file_path,i));
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
         * For U-format files we always start new files with a
         * new buffer.
         */
        if ( (Uformat == FALSE) &&
            ((databufs[i]->data_length == databufs[i]->length) ||
            ((databufs[i]->data_length == databufs[i]->bufendp) &&
             (databufs[i]->end_of_tpfile == TRUE ||
              databufs[i]->last_buffer == TRUE))) ) {
            databufs[i]->flag = BUFFER_FULL;
        } else if ( Uformat == TRUE ) {
            databufs[i]->flag = BUFFER_FULL;
        }
        /*
         * Signal and release this buffer
         */
        rc = Cthread_cond_broadcast_ext(databufs[i]->lock);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"DiskToMemory() Cthread_cond_broadcast_ext(): %s\n",
                sstrerror(serrno));
            return(-1);
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
        if ( rtcpd_CheckProcError() & RTCP_FAILED ) break;
    } /* for (;;) */
    
    return(0);
}

    tape_list_t *tape, *tl;
    tape_list_t *tape;
    file_list_t *file;
    rtcpFileRequest_t *filereq;
    diskIOstatus_t *diskIOstatus = NULL;
    u_signed64 nbbytes;
    char *p, u64buf[22];
    int pool_index = -1;
    int indxp = 0;
    int offset = 0;
    int disk_fd = -1;
    int last_file = FALSE;
    int end_of_tpfile = FALSE;
    int rc, mode, severity, save_errno,save_serrno;
    int rc, save_serrno;
    rtcp_log(LOG_DEBUG,"diskIOthread() started\n");
        rtcp_log(LOG_ERR,"diskIOthread() received NULL argument\n");
        rtcpd_SetProcError(RTCP_FAILED);
        DiskIOfinished();
    }

    tape = ((thread_arg_t *)arg)->tape;
    file = ((thread_arg_t *)arg)->file;
    client = ((thread_arg_t *)arg)->client;
    pool_index = ((thread_arg_t *)arg)->pool_index;
    indxp = ((thread_arg_t *)arg)->start_indxp;
    offset = ((thread_arg_t *)arg)->start_offset;
    last_file = ((thread_arg_t *)arg)->last_file;
    end_of_tpfile = ((thread_arg_t *)arg)->end_of_tpfile;

    if ( file == NULL || tape == NULL ) {
        rtcp_log(LOG_ERR,"diskIOthread() received NULL tape/file element\n");
        rtcpd_SetProcError(RTCP_FAILED);
        (void) tellClient(&client_socket,NULL,NULL,-1); 
        DiskIOfinished();
    }
    diskIOstatus = &proc_stat.diskIOstatus[pool_index];
    tapereq = &tape->tapereq;
    filereq = &file->filereq;
    mode = tapereq->mode;
    /*
     * Open the disk file given the specified record format (U/F).
     */
    rc = 0;
    disk_fd = DiskFileOpen(pool_index,tape,file);
    if ( disk_fd == -1 ) {
        tellClient(&client_socket,NULL,file,disk_fd);
        rtcp_CloseConnection(&client_socket);
        return((void *)&failure);
    }
    
    if ( tapereq->mode == WRITE_DISABLE ) {
        rc = MemoryToDisk(disk_fd,pool_index,&indxp,&offset,
                          &last_file,&end_of_tpfile,tape,file);
    } else {
        rc = DiskToMemory(disk_fd,pool_index,&indxp,&offset,
                          &last_file,&end_of_tpfile,tape,file);
    }
    save_serrno = serrno;
    filereq->TEndTransferDisk = (int)time(NULL);
    if ( rc == -1 ) {
        tellClient(&client_socket,NULL,file,rc);
        if ( *filereq->stageID != '\0' &&
             tapereq->mode == WRITE_DISABLE &&
             save_serrno == ENOSPC ) {
            /*
             * An disk overflow. Interrupt the whole current processing
             * and tell main control thread to restart with this
             * disk file
             */
            rtcpd_SetProcError(RTCP_FAILED | RTCP_LOCAL_RETRY);
        } else {
            /*
             * Fatal
             */
            rtcpd_SetProcError(RTCP_FAILED);
        }
        rtcp_CloseConnection(&client_socket);
        /*
         * In case of copy failure, we don't close the file
         * in MemoryToDisk() or DiskToMemory)().
         */
        (void)DiskFileClose(disk_fd,pool_index,tape,file);
        return((void *)&failure);

    if ( mode == WRITE_ENABLE && (filereq->concat & VOLUME_SPANNING) != 0 ) {
    /*
     * File is already closed in MemoryToDisk() or DiskToMemory()
     */
    if ( tapereq->mode == WRITE_DISABLE ) {
        /*
         * If we are reading from tape, we must tell the
         * stager that the data has successfully arrived at
         * its final destination. Note, for tape write it is
         * the tape IO thread who will update the stager.
         */
        filereq->proc_status = RTCP_FINISHED;
        (void)rtcpd_stageupdc(tape,file);
        DK_STATUS(RTCP_PS_NOBLOCKING);

        DEBUG_PRINT((LOG_DEBUG,"diskIOthread() fseq %d <-> %s copied %d bytes, rc=%d\n",
            file->filereq.tape_fseq,file->filereq.file_path,
            (unsigned long)file->filereq.bytes_out,file->filereq.cprc));
        tellClient(&client_socket,NULL,file,rc);
    }

    tellClient(&client_socket,NULL,NULL,0);
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
        *poolsize = 5;         /* Set some reasonable default */
    }
    
    rc = Cpool_create(*poolsize,poolsize);
    rtcp_log(LOG_DEBUG,"rtcpd_InitDiskIO() thread pool (id=%d): pool size = %d\n",
    rtcp_log(LOG_INFO,"rtcpd_InitDiskIO() thread pool (id=%d): pool size = %d\n",
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

int rtcpd_CleanUpDiskIO(int poolID) {
int rtcpd_CleanUpDiskIO(int poolID) {
    if ( thargs != NULL ) free(thargs);
    return(0);
}


                      tape_list_t *tape,
                      file_list_t *file,
                      int poolID, int poolsize) {

    tape_list_t *nexttape;
    tape_list_t *nexttape, *prevtape;
    u_signed64 prev_filesz;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    diskIOstatus_t *diskIOstatus;
    thread_arg_t *tharg;
    thread_arg_t *tharg;
    int rc, indxp, offset, last_file,end_of_tpfile;
    int prev_bufsz, next_nb_bufs, next_bufsz, thIndex;
    register int convert;

    if ( client == NULL || tape == NULL || file == NULL ||
        poolsize <= 0 ) {
        serrno = EINVAL;
        return(-1);
    }

    rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO() called\n");
    DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO() called\n"));
     * Reserve a thread argument table
     */
    thargs = (thread_arg_t *)malloc(poolsize * sizeof(thread_arg_t));
    if ( thargs == NULL ) {
        save_serrno = errno;
            sstrerror(errno));
        serrno = save_serrno;
        rtcpd_SetProcError(RTCP_FAILED);

    /*
     * Loop over all file requests
     */
    indxp = offset = 0;
    last_file = FALSE;
    prevfile = NULL;
    /*
    prevtape = NULL;
     * We can safely set diskIOstarted flag here. It is only used
     * by us and the diskIOthread's we will start in the following.
     * It must be set to one to allow the first diskIOthread to
     * start.
     */
    proc_cntl.diskIOstarted = 1;
    proc_cntl.diskIOfinished = 0;
    CLIST_ITERATE_BEGIN(tape,nexttape) {
        tapereq = &nexttape->tapereq;
        CLIST_ITERATE_BEGIN(nexttape->file,nextfile) {
            end_of_tpfile = FALSE;
            filereq = &nextfile->filereq;
            Uformat = (*filereq->recfm == 'U' ? TRUE : FALSE);
            if ( nexttape->next == tape && 
                 nextfile->next == nexttape->file ) last_file = TRUE;
            if ( last_file == TRUE || (nextfile->next->filereq.concat & 
                 (NOCONCAT | NOCONCAT_TO_EOD)) != 0 ) end_of_tpfile = TRUE;
            if ( nextfile->filereq.proc_status != RTCP_FINISHED ) {
                /*
                 * Get control info
                 */
                rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
                    save_serrno = serrno;
                    rtcpd_AppendClientMsg(NULL, nextfile, "Cannot lock mutex: %s\n",
                        sstrerror(serrno));
                    rtcpd_SetReqStatus(NULL,nextfile,serrno,
                        RTCP_FAILED | RTCP_RESELECT_SERV);
                    rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cthread_mutex_lock_ext(): %s\n",
                        sstrerror(serrno));
                    rtcpd_SetProcError(RTCP_FAILED);
                    free(thargs);
                }
                /*

                 * Check if we need to exit due to processing error
                 */
                rc= rtcpd_CheckProcError();
                if ( rc != RTCP_OK && rc != RTCP_RETRY_OK ) {
                    rtcp_log(LOG_ERR,"rtcp_StartDiskIO() processing error detected\n");
                    proc_cntl.diskIOfinished = 1;
                    free(thargs);
                    return(-1);
            }
                 * On tape write we know the next file size
                 * Wait while buffers are overallocated. For tape write
                 * there are two additional conditions:
                 * 1) we must make sure that the previously dispatched
                 *    thread has started (i.e. it has locked its first
                 *    buffer).
                 * 2) we must check if we need to wait for the 
                 *    Ctape_info() for more info. (i.e. blocksize)
                 *    before starting the disk -> memory copy.
                next_bufsz = rtcpd_CalcBufSz(nexttape,nextfile);
                while ( proc_cntl.nb_reserved_bufs >= nb_bufs ||
                       ((tapereq->mode == WRITE_ENABLE) &&
                        ((proc_cntl.diskIOstarted == 0) ||
                         (filereq->blocksize < 0))) ) {
                    rc = Cthread_cond_wait_ext(proc_cntl.cntl_lock);
                    if ( rc == -1 ) {
                        rtcpd_AppendClientMsg(NULL, nextfile, "Error on condition wait: %s\n",
                            sstrerror(serrno));
                        rtcpd_SetReqStatus(NULL,nextfile,serrno,
                            RTCP_FAILED | RTCP_RESELECT_SERV);
                        rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cthread_cond_wait_ext(proc_cntl): %s\n",
                            sstrerror(serrno));
                        (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                        rtcpd_SetProcError(RTCP_FAILED);
                        free(thargs);
                        return(-1);
                    }
                    /*
                     * Check if we need to exit due to processing error
                     */
                    rc= rtcpd_CheckProcError();
                    if ( rc != RTCP_OK && rc != RTCP_RETRY_OK ) {
                        rtcp_log(LOG_ERR,"rtcp_StartDiskIO() processing error detected\n");
                        (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                        free(thargs);
                        return(-1);
                    }
                }
                proc_cntl.diskIOstarted = 0;
                 * On tape read we don't know the file size. Make
                 * Calculate nb buffers to be used for the next file.
                next_nb_bufs = nb_bufs + 1;
                if ( nexttape->tapereq.mode == WRITE_ENABLE ) {
                    /*
                     * On tape write we know the next file size
                     */
                    next_bufsz = rtcpd_CalcBufSz(nexttape,nextfile);
                    if ( filereq->concat == CONCAT ) 
                        next_nb_bufs = (int)(((u_signed64)offset + 
                                nextfile->filereq.bytes_in) / (u_signed64)next_bufsz);
                    else 
                        next_nb_bufs = (int)(nextfile->filereq.bytes_in / 
                                (u_signed64)next_bufsz);
                    /*
                     * Increment by one if not concatenating or we reached last
                     * file in the concatenation (subsequent file will start
                     * with a new buffer).
                     */
                    if ( nextfile->next->filereq.concat == NOCONCAT )
                        next_nb_bufs++;
                } else {
                    /*
                     * On tape read we don't know the file size. Make
                     * sure to overallocate until we know the exact size
                     * set by the tape IO thread at end of file.
                     */
                    next_nb_bufs = nb_bufs + 1;
                }
                DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO() number of reserved buffs %d + %d\n",
                    proc_cntl.nb_reserved_bufs,next_nb_bufs));
            proc_cntl.nb_reserved_bufs += next_nb_bufs;
                proc_cntl.nb_reserved_bufs += next_nb_bufs;
                DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO() new number of reserved buffs %d\n",
                    proc_cntl.nb_reserved_bufs));
            rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                if ( rc == -1 ) {
                    rtcpd_AppendClientMsg(NULL,nextfile,"Cannot unlock CNTL mutex: %s\n",
                        sstrerror(serrno));
                    rtcpd_SetReqStatus(NULL,nextfile,serrno,
                        RTCP_FAILED | RTCP_RESELECT_SERV);
                    rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cthread_mutex_unlock_ext(): %s\n",
                        sstrerror(serrno));
                    free(thargs);
                    return(-1);
                }
            /* 
                /* Update offset
                 * in buffer table.
                indxp = offset = 0;
                if ( prevtape == NULL || prevfile == NULL ) {
                    /*
                     * First file
                     */
                    indxp = offset = 0;
                } else {
                    prev_bufsz = rtcpd_CalcBufSz(prevtape,prevfile);
                    if ( prevtape->tapereq.mode == WRITE_ENABLE )
                        prev_filesz = prevfile->filereq.bytes_in;
                    else
                        prev_filesz = prevfile->filereq.bytes_out;
                    DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO() prev. file size %d, buffer sz %d, indxp %d\n",
                        (int)prev_filesz,prev_bufsz,indxp));
                    indxp = (indxp + (int)(((u_signed64)offset + prev_filesz) /
                        ((u_signed64)prev_bufsz)));
                    DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO() new indxp %d\n",
                        indxp));
                    if ( tapereq->mode == WRITE_DISABLE ) {
                         * Not concatenating on tape. Start next file
                         * On tape read we need to check if the tape mark 
                         * of previous file was detected in the next buffer. 
                         * This can happen if the last block of file ended 
                         * up as the last block of a buffer.
                        rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO(): no concatenate (%d != %d), indxp %d\n",
                        DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO() prev sz %d, prev. bufsz %d, spill %d, blksiz %d, indxp %d\n",
                            (int)prev_filesz,prev_bufsz,(prev_bufsz - (int)(prev_filesz % (u_signed64)prev_bufsz)),
                            prevfile->filereq.blocksize,indxp));

                        if ( (prev_bufsz - (int)(prev_filesz % (u_signed64)prev_bufsz)) <
                             prevfile->filereq.blocksize ) indxp++;
                        DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO() New indxp %d\n",indxp));
                         * On tape write we need offset if we are
                         * On tape read we always start with a brand
                         * new buffer except if previous file was empty
                        rtcp_log(LOG_DEBUG,"rtcpd_StartDiskIO(): concatenate (%d == %d)\n",
                        if ( prev_filesz != 0 ) indxp++;
                        offset = 0;
                    }
                    indxp = indxp % nb_bufs;
                    if ( tapereq->mode == WRITE_ENABLE ) {
                        if ( (filereq->concat == NOCONCAT) ||
                             (Uformat == TRUE) ) {
                            /*
                             * Not concatenating on tape. Start next file
                             * with a brand new buffer except if previous
                             * file was empty we re-use the previous buffer.
                             * Also U-format files start with new buffer
                             * since since in this case concatenation does 
                             * not imply any problems with partial blocks.
                             */
                            DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO(): no concatenate (%d != %d), indxp %d\n",
                                nextfile->filereq.tape_fseq,prevfile->filereq.tape_fseq,indxp));

                            if ( prev_filesz != 0 ) 
                                indxp = (indxp + 1) % nb_bufs;
                            offset = 0;
                        } else {
                            /*
                             * On tape write we need offset if we are
                             * concatenating on tape
                             */
                            DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO(): concatenate (%d == %d)\n",
                                nextfile->filereq.tape_fseq,prevfile->filereq.tape_fseq));
                            offset = (int)(((u_signed64)offset + prev_filesz) %
                                ((u_signed64)prev_bufsz));
                        }
                }
            }
                prevtape = nexttape;
                prevfile = nextfile;
                /*
                 * Get next thread index and fill in thread args
                 */
                thIndex = Cpool_next_index(poolID);
                if ( thIndex == -1 ) {
                    rtcpd_AppendClientMsg(NULL, nextfile, "Error assigning thread: %s\n",
                        sstrerror(serrno));
                    rtcpd_SetReqStatus(NULL,nextfile,serrno,
                        RTCP_FAILED | RTCP_RESELECT_SERV);
                    rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() Cpool_next_index(): %s\n",
                        sstrerror(serrno));
                    return(-1);
                }
                tharg = &thargs[thIndex];
                /*
                 * Open a separate connection to client (to be closed 
                 * by the thread at end of processing).
                 */
                tharg->client_socket = INVALID_SOCKET;
                rc = rtcpd_ConnectToClient(&tharg->client_socket,
                                           client->clienthost,
                                           &client->clientport);
                if ( rc == -1 ) {
                    rtcp_log(LOG_ERR,"rtcpd_StartDiskIO() rtcp_ConnectToClient(%s,%d): %s\n",
                        client->clienthost,client->clientport,sstrerror(serrno));
                    free(thargs);
                    return(-1);
                }

                tharg->tape = nexttape;
                tharg->file = nextfile;
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
                DEBUG_PRINT((LOG_DEBUG,"rtcpd_StartDiskIO(thIndex=%d,arg=0x%lx) start with indxp=%d, offset=%d, end_of_tpfile=%d\n",
                    thIndex,tharg,indxp,offset,end_of_tpfile));
                rc = Cpool_assign(poolID,diskIOthread,(void *)tharg,-1);

            } /* if ( ... ) */
        } CLIST_ITERATE_END(nexttape->file,nextfile);
    } CLIST_ITERATE_END(tape,nexttape);
    return(0);
}

