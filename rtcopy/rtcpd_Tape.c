/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd_Tape.c - RTCOPY server tape IO thread
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <io.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#include <sys/times.h>
#endif /* _WIN32 */

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
#include <Cthread_api.h>
#include <vdqm_api.h>
#include <Ctape_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <serrno.h>
char *getconfent _PROTO((char *, char *, int));


#define TP_STATUS(X) (proc_stat.tapeIOstatus.current_activity = (X))
#define TP_SIZE(X)   (proc_stat.tapeIOstatus.nbbytes += (u_signed64)(X))
#define DEBUG_PRINT(X) {if ( debug == TRUE ) rtcp_log X ;}

typedef struct thread_arg {
    SOCKET client_socket;
    tape_list_t *tape;
    rtcpClientInfo_t *client;
} thread_arg_t;

extern int Debug;

extern int nb_bufs;

extern int bufsz;

extern processing_cntl_t proc_cntl;
extern processing_status_t proc_stat;

extern buffer_table_t **databufs;

extern rtcpClientInfo_t *client;

extern int success;
extern int failure;
extern int AbortFlag;

static int last_block_done = 0;
static int WaitToJoin = FALSE;

#ifdef MONITOR /* Added for Monitoring */
time_t lasttime_monitor_msg_sent=0;
u_signed64 last_qty_sent = 0;
int nb_files = 0;
int current_file = 0;
long clktck;
#endif

/*
 * Signal to disk IO thread that file has been positioned (tape read with
 * stager only).
 */
int rtcpd_SignalFilePositioned(tape_list_t *tape, file_list_t *file) {
    int rc;

    rtcp_log(LOG_DEBUG,"rtcpd_SignalFilePositioned() called\n");
    if ( tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_SignalFilePositioned(): Cthread_mutex_lock_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    rc = Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_SignalFilePositioned(): Cthread_cond_broadcast_ext(proc_cntl): %s\n",
            sstrerror(serrno));
        (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
        return(-1);
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_SignalFilePositioned(): Cthread_mutex_unlock_ext(proc_cntl): %s\n",
                 sstrerror(serrno));
        return(-1);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_SignalFilePositioned() returns\n");
    return(0);
}

static int WaitDiskIO() {
    int rc;

    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    if ( rc == -1 ) return(-1);

    rtcp_log(LOG_DEBUG,"WaitDiskIO(): diskIOfinished=%d, nb_diskIOactive=%d\n",
             proc_cntl.diskIOfinished,proc_cntl.nb_diskIOactive);
    while ( proc_cntl.diskIOfinished == 0 || proc_cntl.nb_diskIOactive > 0 ) {
        rc = Cthread_cond_wait_ext(proc_cntl.cntl_lock);
        if ( rc == -1 ) return(-1);
        rtcp_log(LOG_DEBUG,"WaitDiskIO(): diskIOfinished=%d, nb_diskIOactive=%d\n",
                 proc_cntl.diskIOfinished,proc_cntl.nb_diskIOactive);
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    return(rc);
}

static void TapeIOstarted() {
    (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    proc_cntl.tapeIOstarted = 1;
    proc_cntl.tapeIOfinished = 0;
    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    return;
}

static void TapeIOfinished() {
    (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    proc_cntl.tapeIOstarted = 0;
    proc_cntl.tapeIOfinished = 1;
    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    return;
}

int rtcpd_nbFullBufs(int increment) {
    int rc;
    (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    proc_cntl.nbFullBufs += increment;
    rc = proc_cntl.nbFullBufs;
    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    return(rc);
}

/*
 * Read from tape to memory
 */
static int MemoryToTape(int tape_fd, int *indxp, int *firstblk,
                        int *diskIOfinished,
                        tape_list_t *tape, 
                        file_list_t *file) {
    int nb_bytes, rc, i, j, last_sz, blksiz, severity, lrecl;
    int end_of_tpfile, buf_done, nb_truncated, spill, bytes_used, proc_err;
    int pileupWaitTime = 0, nbFullBufs;
    char *convert_buffer = NULL;
    void *convert_context = NULL;
    char *p;
    register int Uformat;
    register int debug = Debug;
    register int convert, NoSyncAccess;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;

    if ( tape_fd < 0 || indxp == NULL || firstblk == NULL ||
         diskIOfinished == NULL || tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    tapereq = &tape->tapereq;
    filereq = &file->filereq;
    if ( last_block_done != 0 ) {
        rtcp_log(LOG_ERR,"MemoryToTape() called beyond last block!!!\n");
        return(-1);
    }

    blksiz = filereq->blocksize;
    lrecl = filereq->recordlength;
    Uformat = (*filereq->recfm == 'U' ? TRUE : FALSE);
    if ( (lrecl <= 0) && (Uformat == FALSE) ) lrecl = blksiz;
    else if ( Uformat == TRUE ) lrecl = 0;

    convert = filereq->convert;
    nb_truncated = spill = 0;
    NoSyncAccess = *diskIOfinished;

    if ( ((p = getenv("RTCPD_PILEUP_WAIT_TIME")) != NULL) ||
         ((p = getconfent("RTCPD","PILEUP_WAIT_TIME",0)) != NULL) )
      pileupWaitTime = atoi(p);

    /*
     * Write loop to break out of
     */
    end_of_tpfile = FALSE;
    proc_err = 0;
    for (;;) {
        i = *indxp;
        buf_done = FALSE;
        /*
         * Synchronize access to next buffer
         */
        if ( NoSyncAccess == 0 ) {
            TP_STATUS(RTCP_PS_WAITMTX);
            nbFullBufs = rtcpd_nbFullBufs(0);
            TP_STATUS(RTCP_PS_WAITMTX);
            if ( (nbFullBufs < 1) && (pileupWaitTime > 0) ) {
              rtcp_log(LOG_INFO,"Tape IO protection against disk IO slack: index=%d, nb full buffers=%d, wait %d seconds\n",
                       i,
                       nbFullBufs,
                       pileupWaitTime);
              rtcp_log(LOG_INFO,"path=%s\n",filereq->file_path);
              sleep(pileupWaitTime);
            }
            TP_STATUS(RTCP_PS_WAITMTX);
            rc = Cthread_mutex_lock_ext(databufs[i]->lock);
            TP_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToTape() Cthread_mutex_lock_ext(): %s\n",
                    sstrerror(serrno));
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                }
                return(-1);
            }
        }
        /*
         * Wait until it is full
         */
        while (databufs[i]->flag == BUFFER_EMPTY) {
            rtcpd_CheckReqStatus(NULL,file,NULL,&severity);
            if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
                  (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                    convert_buffer = NULL;
                    convert_context = NULL;
                }
                break;
            }

            if ( NoSyncAccess == 1 ) {
                rtcp_log(LOG_ERR,"MemoryToTape() unexpected empty buffer %d after diskIO finished\n",i);
                (void)rtcpd_SetReqStatus(NULL,file,SEINTERNAL,
                                         RTCP_FAILED|RTCP_UNERR);
                (void)rtcpd_AppendClientMsg(NULL,file,"Internal error: %s\n",
                                           "unexpected empty buffer");
                return(-1);
            }
            databufs[i]->nb_waiters++;
            TP_STATUS(RTCP_PS_WAITCOND);
            rc = Cthread_cond_wait_ext(databufs[i]->lock);
            TP_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToTape() Cthread_cond_wait_ext(): %s\n",
                    sstrerror(serrno));
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                }
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
            databufs[i]->nb_waiters--;
        } /*  while (databufs[i]->flag == BUFFER_EMPTY) */

        if ( databufs[i]->flag == BUFFER_FULL ) {
            rtcpd_CheckReqStatus(NULL,file,NULL,&severity);
            if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
                  (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                    convert_buffer = NULL;
                    convert_context = NULL;
                }
            }
        }
        if ( proc_err != 0 ) break;
        DEBUG_PRINT((LOG_DEBUG,"MemoryToTape() buffer %d full\n",i));

        /*
         * Verify that actual buffer size matches block size
         */
        if ( (databufs[i]->length % blksiz) != 0 ) {
            rtcp_log(LOG_ERR,"MemoryToTape() blocksize mismatch\n");
            if ( NoSyncAccess == 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            }
            if ( (convert & FIXVAR) != 0 ) {
                if ( convert_buffer != NULL ) free(convert_buffer);
                if ( convert_context != NULL ) free(convert_context);
            }
            (void)rtcpd_SetReqStatus(NULL,file,SEINTERNAL,
                                     RTCP_FAILED|RTCP_UNERR);
            (void)rtcpd_AppendClientMsg(NULL,file,"Internal error: %s\n",
                                        "MemoryToTape() blocksize mismatch");
            return(-1);
        }
        if ( ((convert & FIXVAR) != 0) && (convert_buffer == NULL) ) {
            convert_buffer = (char *)malloc(blksiz);
            if ( convert_buffer == NULL ) {
                (void)rtcpd_SetReqStatus(NULL,file,errno,
                                         RTCP_RESELECT_SERV);
                (void)rtcpd_AppendClientMsg(NULL,file,RT105,sstrerror(errno));
                rtcp_log(LOG_ERR,"MemoryToTape() malloc(): %s\n",
                    sstrerror(errno));
                if ( NoSyncAccess == 0 ) {
                    (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                    (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                }
                return(-1);
            }
        }
        /*
         * Check if this is the last buffer of the tape file
         */
        end_of_tpfile = databufs[i]->end_of_tpfile;
        if ( end_of_tpfile == TRUE ) 
            DEBUG_PRINT((LOG_DEBUG,"MemoryToTape() end of tape file in buffer %d\n",i));
        /*
         * Copy the data to tape
         */
        last_sz = 0;
        if ( filereq->maxnbrec > 0 && (convert & FIXVAR) == 0 ) {
            /*
             * Check if we reached the number of records limit
             * and, if so, reduce the data_length accordingly.
             */
            if ( Uformat == FALSE ) {
                if ( filereq->nbrecs + databufs[i]->data_length/lrecl >
                    filereq->maxnbrec ) {
                    databufs[i]->data_length = (int)(filereq->maxnbrec -
                        filereq->nbrecs) * lrecl;
                    rtcpd_SetReqStatus(NULL,file,ERTLIMBYSZ,RTCP_OK | RTCP_LIMBYSZ);
                }
            } else {
                if ( filereq->nbrecs + databufs[i]->nbrecs > 
                    filereq->maxnbrec ) {
                    databufs[i]->nbrecs = (int)(filereq->maxnbrec -
                        filereq->nbrecs);
                    rtcpd_SetReqStatus(NULL,file,ERTLIMBYSZ,RTCP_OK | RTCP_LIMBYSZ);
                }
            }
        }
        DEBUG_PRINT((LOG_DEBUG,"MemoryToTape() write %d bytes to tape\n",
            databufs[i]->data_length));
        j = *firstblk;
        for (;;) {
            if ( (Uformat == FALSE) || ((Uformat == TRUE) && (convert & NOF77CW) != 0) ) {
                /*
                 * Fix record format
                 */
                if ( (convert & FIXVAR) == 0 ) {
                    /*
                     * Have we read all data in this buffer yet?
                     */
                    if ( j*blksiz >= databufs[i]->data_length + 
                                     (*firstblk) * blksiz ) {
                        DEBUG_PRINT((LOG_DEBUG,"MemoryToTape() reached EOB (buffer %d), j=%d, blksiz=%d, data_length=%d, firstblk=%d\n",
                                i,j,blksiz,databufs[i]->data_length,*firstblk));
                        break;
                    }
                    /*
                     * Normal no record padding format.
                     * Note: last block of file may be partial.
                     */
                    nb_bytes = databufs[i]->data_length - (j-*firstblk)*blksiz;
                    if ( nb_bytes > blksiz ) nb_bytes = blksiz;
                    convert_buffer = databufs[i]->buffer + j*blksiz;
                } else {
                    /*
                     * Record padding (blocking) format: input records
                     * are normal text lines with newlines, output
                     * records are fixed length (lrecl) where the new
                     * line has been replaced with spaces (' ') padded
                     * up to lrecl
                     */
                    for (;;) {
                        nb_bytes = rtcpd_VarToFix(
                            databufs[i]->buffer + (*firstblk)*blksiz,
                            convert_buffer,
                            databufs[i]->data_length - (*firstblk)*blksiz,
                            blksiz,lrecl,&bytes_used,
                            &nb_truncated,&convert_context);

                        if ( nb_bytes > 0 ) filereq->nbrecs++;
                        if ( filereq->maxnbrec > 0 &&
                             filereq->nbrecs > filereq->maxnbrec ) {
                            spill = nb_bytes = 0;
                            rtcpd_SetReqStatus(NULL,file,ERTLIMBYSZ,RTCP_OK | 
                                                               RTCP_LIMBYSZ);
                            break;
                        }
                        file->tapebytes_sofar += (u_signed64)bytes_used;
                        if ( nb_bytes == blksiz ) {
                            spill = 0;
                            break;
                        } else if ( nb_bytes > 0 ) {
                            spill = nb_bytes;
                            nb_bytes = 0;
                        } else if ( nb_bytes <= 0 ) break;
                    }
                    /*
                     * Break out to get a new buffer
                     */
                    if ( nb_bytes <= 0 ) break;
                }
            } else {
                /*
                 * FORTRAN sequential access variable length records.
                 * Have we read all data in this buffer yet?
                 */
                if ( j*blksiz >= databufs[i]->data_length ) break;
                nb_bytes = databufs[i]->lrecl_table[j];
                convert_buffer = databufs[i]->buffer + j*blksiz;
            }

            if ( (convert & EBCCONV) != 0 ) asc2ebc(convert_buffer,nb_bytes);
            /*
             * >>>>>>>>>>> write to tape <<<<<<<<<<<<<
             */
            TP_STATUS(RTCP_PS_WRITE);
            rc = twrite(tape_fd,
                        convert_buffer,
                        nb_bytes,
                        tape,
                        file);
            TP_STATUS(RTCP_PS_NOBLOCKING);
            
	    if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToTape() tape write error\n");
                if ( NoSyncAccess == 0 ) {
                    (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                    (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                }
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                }
                return(-1);
            }

	    if ( rc == 0 ) {
                /*
                 * EOV reached
                 */
                if ( file->eovflag == 1 ) *firstblk = j;
                break;
            }
            last_sz += rc;
            if ( (convert & FIXVAR) == 0 ) {
                file->tapebytes_sofar += (u_signed64)rc;
                if ( lrecl > 0 && rc/lrecl > 0 ) 
                    filereq->nbrecs += (u_signed64)rc/lrecl;
                else if ( rc > 0 ) filereq->nbrecs++;
            }

#ifdef MONITOR  /* Monitoring Code */
 
            {
                int forceSend;
      
                if (file == NULL || filereq == NULL) {
                    rtcp_log(LOG_ERR, "MONITOR - file or filereq is NULL\n");
                } else {
                    forceSend = (file->tapebytes_sofar == 0) || (file->tapebytes_sofar ==  filereq->bytes_in);
                    rtcpd_sendMonitoring(file->tapebytes_sofar, filereq->bytes_in, forceSend);
                }
            }

#endif /* End Monitoring Code */
      

            TP_SIZE(rc);
            j++;
        } /* End of for (j=...) */
        DEBUG_PRINT((LOG_DEBUG,"MemoryToTape() wrote %d bytes to tape\n",
            last_sz));
        /*
         * Subtract what we've just written. If we have reblocked
         * the data the last_sz is larger than the input data size
         * because records have been padded. In that case 
         * rtcpd_VarToFix() returns 0 if all input data has been
         * converted.
         */
        if ( (convert & FIXVAR) == 0 )
            databufs[i]->data_length -= last_sz;
        else
            if ( nb_bytes == 0 ) databufs[i]->data_length = 0;

        /*
         * Reset the buffer semaphore only if the
         * full buffer has been succesfully written.
         */
        if ( databufs[i]->data_length <= 0 ) {
            /*
             * Check if this is the request end.
             */
            last_block_done = databufs[i]->last_buffer;
            if ( last_block_done != 0 )
                DEBUG_PRINT((LOG_DEBUG,"MemoryToTape() last block done = %d\n",
                    last_block_done));
            /*
             * Reset all switches so that the buffer can be reused
             */
            databufs[i]->bufendp = 0;
            databufs[i]->data_length = 0;
            databufs[i]->nbrecs = 0;
            databufs[i]->end_of_tpfile = FALSE;
            databufs[i]->last_buffer = FALSE;
            databufs[i]->flag = BUFFER_EMPTY;
            buf_done = TRUE;
            /*
             * Increment the circular data buffer index and reset the
             * tape block index.
             */
            *indxp = (*indxp+1) % nb_bufs;
            *firstblk = 0;
        }
        /*
         * Release lock on this buffer
         */
        if ( databufs[i]->nb_waiters > 0 && NoSyncAccess == 0 ) {
            rc = Cthread_cond_broadcast_ext(databufs[i]->lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToTape() Cthread_cond_broadcast_ext(): %s\n",
                    sstrerror(serrno));
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                }
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
        }
        if ( NoSyncAccess == 0 ) {
            rc = Cthread_mutex_unlock_ext(databufs[i]->lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToTape() Cthread_mutex_unlock_ext(): %s\n",
                    sstrerror(serrno));
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                }
                return(-1);
            }
        }

        if ( buf_done == TRUE && NoSyncAccess == 0 ) {
            (void)rtcpd_nbFullBufs(-1);
            /*
             * Decrement the number of reserved buffers.
             */
            rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToTape() Cthread_mutex_lock_ext(): %s\n",
                    sstrerror(serrno));
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                }
                return(-1);
            }
            proc_cntl.nb_reserved_bufs--;
            /*
             * Check if disk IO has finished. If so, we don't need to sync.
             * buffers anymore. The test on diskIOstarted may seem a bit
             * unlogical but in fact this flag should have been set by the
             * thread started before we can allow no sync. access - there
             * is a window where disk IO starter can exit before the last
             * disk IO thread has started. 
             */
            if ( proc_cntl.diskIOfinished == 1 && 
                 proc_cntl.diskIOstarted == 1 &&
                 proc_cntl.nb_diskIOactive == 0 ) {
                rtcp_log(LOG_DEBUG,"MemoryToTape() disk IO has finished! Sync. not needed anymore\n");
                NoSyncAccess = *diskIOfinished = 1;
            }

            /*
             * Signal to main thread that we changed cntl info
             */
            if ( proc_cntl.nb_reserved_bufs < nb_bufs ) {
                rc = Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                if ( rc == -1 ) {
                    rtcp_log(LOG_ERR,"MemoryToTape() Cthread_cond_broadcast_ext(proc_cntl): %s\n",
                        sstrerror(serrno));
                    if ( (convert & FIXVAR) != 0 ) {
                        if ( convert_buffer != NULL ) free(convert_buffer);
                        if ( convert_context != NULL ) free(convert_context);
                    }
                    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                    return(-1);
                }
            }

            rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"MemoryToTape() Cthread_mutex_unlock_ext(): %s\n",
                    sstrerror(serrno));
                if ( (convert & FIXVAR) != 0 ) {
                    if ( convert_buffer != NULL ) free(convert_buffer);
                    if ( convert_context != NULL ) free(convert_context);
                }
                return(-1);
            }
        }

        /*
         * Break out to main tape IO loop if we have hit 
         * the end of the current tape file
         */
        if ( end_of_tpfile == TRUE ) break;
        /*
         * Check if we reached end of volume. If so, return to
         * let the main tapeIO loop iterate one step in the tape
         * request list. This must be the last file request (in
         * a multi-volume request only the last file can span). Note
         * that we have to start on the same indxp since the last
         * buffer has not been completely written out.
         */
        if ( file->eovflag == 1 ) break;
        /*
         * Check if last block
         */
        if ( last_block_done != 0 ) break;
        /*
         * Has something fatal happened while we were occupied
         * writing to the tape?
         */
        rtcpd_CheckReqStatus(NULL,file,NULL,&severity);
        if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
              (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) {
            break;
        }
    } /* End of for (;;) */

    if ( ((convert & FIXVAR) != 0) && (convert_buffer != NULL) ) {
        /*
         * Write out spill from VarToFix conversion
         */
        rtcp_log(LOG_DEBUG,"MemoryToTape() FIXVAR spill: %d, proc_err=%d\n",
                 spill,proc_err);
        if ( proc_err == 0  && spill > 0 ) {
            if ( (convert & EBCCONV) != 0 ) asc2ebc(convert_buffer,spill);
            rc = twrite(tape_fd,
                        convert_buffer,
                        spill,
                        tape,
                        file);
            TP_SIZE(rc);
        }
        if ( nb_truncated > 0 ) {
            rtcpd_AppendClientMsg(NULL,file,RT222,"CPDSKTP",
                nb_truncated);
        }
        free(convert_buffer);
        if ( convert_context != NULL ) free(convert_context);
    }

    TP_STATUS(RTCP_PS_CLOSE);
    if ( proc_err == 0 ) rc = tclose(tape_fd,tape,file);
    TP_STATUS(RTCP_PS_NOBLOCKING);
    return(rc);
}

static int TapeToMemory(int tape_fd, int *indxp, int *firstblk,
                        int *diskIOfinished,
                        tape_list_t *tape, 
                        file_list_t *file) {
    int nb_bytes, rc, i, j, last_sz, blksiz, current_bufsz;
    int lrecl, end_of_tpfile, break_and_return, severity, proc_err;
    int nb_skipped_blks = 0;
    register int Uformat;
    register int debug = Debug;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    extern char *u64tostr _PROTO((u_signed64, char *, int));

    if ( tape_fd < 0 || indxp == NULL || firstblk == NULL ||
         tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    tapereq = &tape->tapereq;
    filereq = &file->filereq;

    /*
     * In the case when startsize > maxsize, i.e. we are concatenating
     * tape files to a single disk file and the max size has been
     * exceeded, we still need to enter the loop in order to flag a
     * buffer full to wake up the disk IO thread. However no data
     * will be read from tape since tapebytes_sofar exceeds maxsize
     * already from the beginning. See notice further down (immediately
     * before the tread()).
     */
    file->tapebytes_sofar = filereq->startsize;
    blksiz = filereq->blocksize;
    lrecl = filereq->recordlength;
    Uformat = (*filereq->recfm == 'U' ? TRUE : FALSE);
    if ( Uformat == TRUE ) lrecl = 0;

    /*
     * Calculate new actual buffer length
     */
    current_bufsz = rtcpd_CalcBufSz(tape,file);
    if ( current_bufsz == -1 ) return(-1);
    /*
     * Tape read loop to break out of. We must assure that there
     * are at least one empty buffer available for next disk IO
     * thread before signaling end of tape file to main control
     * thread. The reason for this is to avoid conflicting access
     * between two disk IO thread on the same buffer in case the
     * network or remote disk is much slower than the tape IO. Thus
     * we must assure that the buffer following the last tape file
     * buffer is empty which means that the currently active disk IO
     * thread has finished with it.
     */
    end_of_tpfile = FALSE;
    break_and_return = FALSE;
    proc_err = 0;
    for (;;) {
        i = *indxp;
        /*
         * Synchronize access to next buffer
         */
        TP_STATUS(RTCP_PS_WAITMTX);
        rc = Cthread_mutex_lock_ext(databufs[i]->lock);
        TP_STATUS(RTCP_PS_NOBLOCKING);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"TapeToMemory() Cthread_mutex_lock_ext(): %s\n",
                sstrerror(serrno));
            return(-1);
        }
        /*
         * Wait until it is empty.
         */
        while (databufs[i]->flag == BUFFER_FULL) {
            rtcpd_CheckReqStatus(NULL,file,NULL,&severity);
            if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
                  (RTCP_LOCAL_RETRY|RTCP_FAILED | RTCP_RESELECT_SERV))) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                break;
            }

            DEBUG_PRINT((LOG_DEBUG,"TapeToMemory() wait on buffer[%d]->flag=%d\n",
                i,databufs[i]->flag));
            databufs[i]->nb_waiters++;
            TP_STATUS(RTCP_PS_WAITCOND);
            rc = Cthread_cond_wait_ext(databufs[i]->lock);
            TP_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"TapeToMemory() Cthread_cond_wait_ext(): %s\n",
                    sstrerror(serrno));
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
            databufs[i]->nb_waiters--;
        } /* while (databufs[i]->flag == BUFFER_FULL) */

        if ( databufs[i]->flag == BUFFER_EMPTY ) {
            rtcpd_CheckReqStatus(NULL,file,NULL,&severity);
            if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
                  (RTCP_LOCAL_RETRY|RTCP_FAILED | RTCP_RESELECT_SERV))) != 0 ) {
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            }
        }
        if ( proc_err != 0 ) break;
        DEBUG_PRINT((LOG_DEBUG,"TapeToMemory() buffer %d empty\n",i));
        if ( end_of_tpfile == FALSE ) {
            /*
             * Set the actual buffer size to match current block size
             */
            databufs[i]->length = current_bufsz;
            /*
             * Reset switches (may remain old settings from a previous file).
             */
            databufs[i]->end_of_tpfile = FALSE;
            databufs[i]->last_buffer = FALSE;
            /*
             * Copy the data from tape
             */
            last_sz = 0;
            databufs[i]->nbrecs = 0;
            /*
             * If U-format, re-allocate the lrecl_table[] if needed.
             */
            if ( Uformat == TRUE ) rc = rtcpd_AdmUformatInfo(file,i);

            for (j=*firstblk; last_sz+blksiz < databufs[i]->length; j++) {
                /*
                 * Max size may have been exceeded even before we
                 * read any data at all. This happens when we are 
                 * concatenating on disk and max size is already 
                 * exceeded already at entry (i.e. startsize > maxsize).
                 * We still need to flag the buffer full to wake up
                 * the disk IO thread.
                 * Note that in the normal case (where we already have 
                 * read some data) the max size limit is tested after 
                 * the read. In that case the following test is always
                 * false.
                 */
                if ( (filereq->maxsize > 0) && 
                     (file->tapebytes_sofar >= filereq->maxsize) ) {
                    /*
                     * Nothing will be copied!
                     */
                    filereq->bytes_out = 0;
                    end_of_tpfile = TRUE;
                    break;
                }

                /*
                 * Read a block
                 */
                end_of_tpfile = FALSE;
                nb_bytes = blksiz;

                /*
                 * >>>>>>>>>>> read from tape <<<<<<<<<<<<<
                 */
                TP_STATUS(RTCP_PS_READ);
                rc = tread(tape_fd,
                           databufs[i]->buffer + last_sz,
                           nb_bytes,
                           tape,
                           file);

                TP_STATUS(RTCP_PS_NOBLOCKING);
                if ( rc == -1 ) {
                    rtcp_log(LOG_ERR,"TapeToMemory() tape read error\n");
                    (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                    (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                    return(-1);
                }
                if ( rc == 0 ) {
                    rtcpd_CheckReqStatus(NULL,file,NULL,&severity);
                    if ( severity & RTCP_NEXTRECORD ) {
                        /*
                         * Bad block skipped
                         */
                        nb_skipped_blks++;
                    } else if ( file->eovflag == 1 ) {
                        /*
                         * EOV reached. Note that this is the normal EOD
                         * status for a non-labeled volume (since there is
                         * no way to tell the difference in that case). We
                         * allow it also for labelled tapes (strictly this
                         * is an error) to allow users to read only first part
                         * of a spanned volume without getting an error. 
                         * Re-assinging it to an EOF instead of EOV allows 
                         * common treatment of all EOD conditions when
                         * trying to position to the next file.
                         */
                        if ( (filereq->concat & VOLUME_SPANNING) == 0 ) {
                            DEBUG_PRINT((LOG_DEBUG,"TapeToMemory() EOV condition without spanning volume\n"));
                            end_of_tpfile = TRUE;
                            file->eovflag = 0;
                        }
                    } else {
                        /*
                         * EOF reached
                         */
                        end_of_tpfile = TRUE;
                    }
                    break;
                }
                /*
                 * The record length was not given by the user
                 */
                if ( (lrecl <= 0) && (Uformat == FALSE) ) {
                    lrecl = rc;
                    filereq->recordlength = lrecl;
                }

                last_sz += rc;
                file->tapebytes_sofar += rc;
                if ( lrecl > 0 && rc/lrecl > 0 ) filereq->nbrecs += rc/lrecl;
                else if ( rc > 0 ) filereq->nbrecs++;
                if ( Uformat == TRUE ) {
                    databufs[i]->lrecl_table[j] = rc;
                    databufs[i]->nbrecs++;
                }

#ifdef MONITOR  /* Monitoring Code */
                {
                    int forceSend;

                    if (file == NULL || filereq == NULL) {
                        rtcp_log(LOG_ERR, "MONITOR - file or filereq is NULL\n");
                    } else {
                        forceSend = (file->tapebytes_sofar == 0) || 
                                    (file->tapebytes_sofar ==  filereq->bytes_in);
                        rtcpd_sendMonitoring(file->tapebytes_sofar, filereq->bytes_in, forceSend);
                    }
                }
#endif          /* End of Monitoring Code */

                TP_SIZE(rc);
            } /* End of for (j=...) */
            /*  
             * Set the actual data length. Should be the
             * same as the current buffer length except for
             * the last buffer of the file.
             */
            databufs[i]->data_length += last_sz;
            databufs[i]->bufendp = last_sz;

            /*
             * Check if we already reached max number of
             * records. If so we substract buffer sizes accordingly
             * and signal end of file with this buffer
             */
            if ( (filereq->maxnbrec > 0) &&
                 (filereq->nbrecs > filereq->maxnbrec) ) {
                if ( Uformat == FALSE && lrecl > 0 ) {
                    databufs[i]->data_length = databufs[i]->data_length -
                        (int)(filereq->nbrecs - filereq->maxnbrec)*lrecl;
                    file->tapebytes_sofar -= 
                        (filereq->nbrecs - filereq->maxnbrec)*lrecl;
                } else if ( Uformat == TRUE ) {
                    databufs[i]->nbrecs = (int)(filereq->nbrecs - filereq->maxnbrec);
                    databufs[i]->data_length = 0;
                    for (j=0;j<databufs[i]->nbrecs;j++)
                        databufs[i]->data_length +=databufs[i]->lrecl_table[j];
                    file->tapebytes_sofar -= databufs[i]->data_length;
                }
                filereq->nbrecs = filereq->maxnbrec;
                end_of_tpfile = TRUE;
                severity = RTCP_OK | RTCP_LIMBYSZ;
                rtcpd_SetReqStatus(NULL,file,ERTLIMBYSZ,severity);
            }
            /*
             * Check if maxsize has been reached for this file
             */
            if ((filereq->maxsize > 0) &&
                (filereq->maxsize <= file->tapebytes_sofar) ) {
                severity = RTCP_OK | RTCP_LIMBYSZ;
                rtcpd_SetReqStatus(NULL,file,ERTLIMBYSZ,severity);
                end_of_tpfile = TRUE;
            }

            if ( end_of_tpfile == TRUE ) {
                databufs[i]->end_of_tpfile = TRUE;
                /*
                 * Need to close here to get compression
                 * statistics etc. before the disk IO
                 * thread send the info. to the client
                 */
                TP_STATUS(RTCP_PS_CLOSE);
                rc = tclose(tape_fd,tape,file);
                TP_STATUS(RTCP_PS_NOBLOCKING);
                if ( rc == -1 ) {
                    rtcp_log(LOG_ERR,"TapeToMemory() tclose(): %s\n",
                        sstrerror(serrno));
                    (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                    (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                    return(-1);
                }
                tape_fd = -1;

                /*
                 * Note down the actual file size to
                 * allow for start up of next disk IO thread.
                 */
                if ((filereq->maxsize > 0) &&
                    (filereq->maxsize <= file->tapebytes_sofar) ) {
                    /*
                     * Truncated due to user specified max size.
                     * Re-calculate number of bytes out and number
                     * of tape records.
                     */
                    filereq->bytes_out = filereq->maxsize - filereq->startsize;
                    if ( lrecl > 0 )
                        filereq->nbrecs = filereq->bytes_out / lrecl;
                } else
                    filereq->bytes_out = file->tapebytes_sofar -
                        filereq->startsize;
            }

            /*
             * Set the buffer semaphore
             */
            databufs[i]->flag = BUFFER_FULL;
            /*
             * Always start next iteration with a new buffer boundary
             */
            *indxp = (*indxp+1) % nb_bufs;
            *firstblk = 0;
        } else {
            /*
             * End of tape file was reached in previous iteration.
             * At least one empty buffer follows the last written
             * buffer for this tape file. We can therefore signal
             * to the control thread that a new disk IO thread can
             * be started.
             */
            DEBUG_PRINT((LOG_DEBUG,
               "TapeToMemory() tapebytes_sofar=%d, max=%d, start=%d, bytes_out=%d\n",
                (int)file->tapebytes_sofar,(int)filereq->maxsize,
                (int)filereq->startsize,(int)filereq->bytes_out));

            /*
             * Tell main control thread that we finished by
             * resetting the number of reserved buffers.
             */
            TP_STATUS(RTCP_PS_WAITMTX);
            rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
            TP_STATUS(RTCP_PS_NOBLOCKING);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"TapeToMemory() Cthread_mutex_lock_ext(proc_cntl): %s\n",
                    sstrerror(serrno));
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
            proc_cntl.nb_reserved_bufs = 0;
            file->end_index = *indxp;
            /*
             * Signal to main thread that we changed cntl info
             */
            rc = Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"TapeToMemory() Cthread_cond_broadcast_ext(proc_cntl): %s\n",
                    sstrerror(serrno));
                (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
            rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"TapeToMemory() Cthread_mutex_unlock_ext(proc_cntl): %s\n",
                    sstrerror(serrno));
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
            /*
             * Set switch to break out and return after having
             * signalled and released the buffer.
             */
            break_and_return = TRUE;
        }
        if ( databufs[i]->flag == BUFFER_FULL )
            DEBUG_PRINT((LOG_DEBUG,"TapeToMemory() flag buffer %d full\n",i));
        if ( databufs[i]->nb_waiters > 0 ) {
            rc = Cthread_cond_broadcast_ext(databufs[i]->lock);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"TapeToMemory() Cthread_cond_broadcast_ext(): %s\n",
                    sstrerror(serrno));
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
                return(-1);
            }
        }
        rc = Cthread_mutex_unlock_ext(databufs[i]->lock);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"TapeToMemory() Cthread_mutex_unlock_ext(): %s\n",
                sstrerror(serrno));
            return(-1);
        }

        if ( break_and_return == TRUE ) break;

        /*
         * Report if there were skipped blocks
         */
        if ( nb_skipped_blks > 0 ) {
            if ( (severity & RTCP_LIMBYSZ) != 0  ) {
                severity = RTCP_OK | RTCP_TPE_LSZ;
                rtcpd_SetReqStatus(NULL,file,ERTLIMBYSZ,severity);
            } else {
                severity = RTCP_OK | RTCP_BLKSKPD;
                rtcpd_SetReqStatus(NULL,file,ERTBLKSKPD,severity);
            }
        }

        /*
         * Break out to return if there were too many skipped blocks
         */
        if ( nb_skipped_blks > MAXNBSKIPS ) {
            severity = RTCP_FAILED | RTCP_MNYPARY | RTCP_USERR;
            rtcpd_SetReqStatus(NULL,file,ERTMNYPARY,severity);
            return(-1);
        }

        /*
         * If end of volume, break out from inifinite loop
         * and return to main tape I/O thread
         */
        if ( file->eovflag == 1 ) break;

        /*
         * Has something fatal happened while we were occupied
         * reading from the tape?
         */
        rtcpd_CheckReqStatus(NULL,file,NULL,&severity);
        if ( (proc_err = ((severity | rtcpd_CheckProcError()) & 
              (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV))) != 0 ) break;
    } /* for (;;) */
    if ( proc_err == 0 && tape_fd >= 0 ) {
        TP_STATUS(RTCP_PS_CLOSE);
        rc = tclose(tape_fd,tape,file);
        TP_STATUS(RTCP_PS_NOBLOCKING);
    }
    return(rc);
}

/*
 * This horrible macro prevents us to always repeate the same code
 * In addition to the return code from any blocking (or non-blocking) 
 * call we need to check the setting of the global processing error
 * to see if e.g. a disk IO thread has failed.
 * We have to be careful in setting processing error: if we are reading
 * a tape the disk IO is behind us and we have to let it finish with
 * previous file(s). Once it reach the current file it will stop because
 * the file request status is FAILED. On tape write at the other hand
 * we are behind the disk IO so we can safely interrupt everything in
 * case of an error.
 * If request has been aborted, we exit the whole process after having
 * closed (and deleted if WRITE_ENABLE) last tape file and released the drive.
 * There is no reasons to wait for the diskIO before exit, since it can
 * be hung in rfio connections (which very well may be the reason for the
 * abort). Note, that the abort will not work if it is the tape IO which
 * is stuck. In this case the process has to be killed by a signal. 
 */
#define CHECK_PROC_ERR(X,Y,Z) { \
    save_errno = errno; \
    save_serrno = serrno; \
    rtcpd_CheckReqStatus((X),(Y),NULL,&severity); \
    if ( rc == -1 || (severity & (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 || \
        (rtcpd_CheckProcError() & (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 ) { \
         rtcp_log(LOG_ERR,"tapeIOthread() %s, rc=%d, severity=%d, errno=%d, serrno=%d\n",Z,\
                  rc,severity,save_errno,save_serrno); \
         if ( mode == WRITE_ENABLE && \
          (rc == -1 || (severity & (RTCP_FAILED|RTCP_RESELECT_SERV)) != 0) && \
          (rtcpd_CheckProcError() & (RTCP_FAILED|RTCP_RESELECT_SERV)) == 0 ) { \
             if ( (severity & (RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 ) \
                 rtcpd_SetProcError(severity); \
             else if ( (severity & RTCP_LOCAL_RETRY) == 0 ) \
                 rtcpd_SetProcError(RTCP_FAILED); \
         } \
         if ( AbortFlag == 0 ) rtcpd_BroadcastException(); \
         if ( tape_fd != -1 ) tcloserr(tape_fd,nexttape,nextfile); \
         (void)WaitDiskIO(); \
         if ( ((severity|rtcpd_CheckProcError()) & RTCP_LOCAL_RETRY) == 0 ) { \
             if ( mode == WRITE_ENABLE ) { \
                 rtcp_log(LOG_DEBUG,"tapeIOthread() return RC=-1 to client\n");\
                 tmpfile = nextfile; \
                 if ( (rc != -1) && (nextfile != NULL) && (nextfile->next->filereq.concat & CONCAT) != 0 ) { \
                     while (tmpfile->next!=nexttape->file && \
                            (tmpfile->next->filereq.concat & CONCAT) != 0 && \
                            *tmpfile->filereq.err.errmsgtxt == '\0' ) \
                         tmpfile = tmpfile->next; \
                     if ( *tmpfile->filereq.err.errmsgtxt != '\0' ) nextfile = tmpfile; \
                 } \
                if ( rc == 0 && AbortFlag != 0 && (severity & (RTCP_FAILED|RTCP_RESELECT_SERV)) == 0 ) \
                    rtcpd_SetReqStatus(X,Y,(AbortFlag == 1 ? ERTUSINTR : ERTOPINTR),rtcpd_CheckProcError()); \
                 (void) tellClient(&client_socket,X,Y,-1); \
                 (void) rtcpd_stageupdc(nexttape,(tmpfile!=NULL?tmpfile:nextfile)); \
                 (void)rtcp_WriteAccountRecord(client,nexttape,(tmpfile!=NULL?tmpfile:nextfile),RTCPEMSG); \
             } \
             (void)rtcpd_FreeBuffers(); \
             (void)rtcpd_Release((X),NULL); \
         } else { \
             tape->local_retry++; \
             if ( mode == WRITE_ENABLE ) { \
                 (void) tellClient(&client_socket,X,Y,0); \
                 (void)rtcpd_WaitCompletion(nexttape,nextfile); \
                 rtcpd_SetProcError(severity); \
                 (void)rtcp_WriteAccountRecord(client,nexttape,nextfile,RTCPEMSG); \
             }\
         } \
         (void) tellClient(&client_socket,NULL,NULL,-1); \
         rtcp_CloseConnection(&client_socket); \
         if ( AbortFlag != 0 ) { \
             if ( mode == WRITE_DISABLE ) { \
                 if ( (nexttape == NULL || nexttape->tapereq.tprc == 0) && \
                      (nextfile == NULL || nextfile->filereq.cprc == 0) ) \
                     rtcpd_SetReqStatus(X,Y,(AbortFlag == 1 ? ERTUSINTR : ERTOPINTR),rtcpd_CheckProcError()); \
                 (void)rtcp_WriteAccountRecord( \
                       client,nexttape,nextfile,RTCPEMSG); \
             } \
             if ( WaitToJoin == FALSE ) exit(0); \
         } \
         if ( rc == -1 ) return((void *)&failure); \
         else return((void *)&success); \
    }}

void *tapeIOthread(void *arg) {
    rtcpClientInfo_t *client;
    tape_list_t *tape, *nexttape;
    file_list_t *nextfile, *tmpfile, *prevfile;
    SOCKET client_socket;
    char *p, u64buf[22];
    u_signed64 bytes_in,totsz;
    int indxp = 0;
    int firstblk = 0;
    int tape_fd = -1;
    int rc,BroadcastInfo,mode,diskIOfinished,severity,save_errno,save_serrno;
    int save_rc;
    extern char *u64tostr _PROTO((u_signed64, char *, int));


#ifdef MONITOR
    clktck = sysconf(_SC_CLK_TCK); 
#endif

    if ( arg == NULL ) {
        rtcp_log(LOG_ERR,"tapeIOthread() received NULL argument\n");
        rtcpd_SetProcError(RTCP_FAILED);
        return((void *)&failure);
    }
    tape = ((thread_arg_t *)arg)->tape;
    client = ((thread_arg_t *)arg)->client;
    client_socket = ((thread_arg_t *)arg)->client_socket;
    free(arg);

    if ( tape == NULL ) {
        rtcp_log(LOG_ERR,"tapeIOthread() received NULL tape request\n");
        rtcpd_SetProcError(RTCP_FAILED);
        return((void *)&failure);
    }

    nexttape = NULL;
    nextfile = NULL;
    bytes_in = 0;
    diskIOfinished = 0;
    mode = tape->tapereq.mode;
    /*
     * Set start flag
     */
    TapeIOstarted();

    /*
     * Initialize the tapeIO processing status
     */
    proc_stat.tapeIOstatus.nbbytes = 0;
    TP_STATUS(RTCP_PS_NOBLOCKING);

    /*
     * Reset flag for last block. On UNIX we are normally in a newly
     * forked process but this is not the case on NT (where we currently
     * only support one request at the time).
     */
    last_block_done = 0;

    if ( tape->local_retry == 0 ) {
        /*
         * Initialize Ctape error message buffer
         */
        rc = rtcpd_CtapeInit();
        if ( rc == -1 ) {
            CHECK_PROC_ERR(tape,NULL,"rtcpd_CtapeInit() error");
        }
#if defined(CTAPE_DUMMIES)
        /*
         * If we run with dummy Ctape we need to assign the
         * drive here
         */
        rc = rtcpd_Assign(tape);
        if ( rc == -1 ) {
            (void)rtcpd_Deassign(-1,&tape->tapereq,NULL);
        }
        CHECK_PROC_ERR(tape,NULL,"rtcpd_Assign() error");
#endif /* CTAPE_DUMMIES */
        /*
         * Reserve the unit (client should have made sure that all
         * requested volumes are in same dgn).
         */
        TP_STATUS(RTCP_PS_RESERVE);
        rc = rtcpd_Reserve(tape);
        TP_STATUS(RTCP_PS_NOBLOCKING);
        if ( rc == -1 ) {
            (void)rtcpd_Deassign(-1,&tape->tapereq,NULL);
        }
        CHECK_PROC_ERR(tape,NULL,"rtcpd_Reserv() error");
    }
    /*
     * Main loop over all tapes requested
     */
    totsz = 0;
    prevfile = tape->file;
    CLIST_ITERATE_BEGIN(tape,nexttape) {
        /*
         * Mount the volume
         */
        if ( (nexttape->local_retry == 0) && 
             ((nexttape == tape) || (prevfile->eovflag == 1)) ) {
            TP_STATUS(RTCP_PS_MOUNT);
            rc = rtcpd_Mount(nexttape);
            TP_STATUS(RTCP_PS_NOBLOCKING);
            save_serrno = serrno; save_errno = errno;
            if ( rc == -1 ) (void)rtcpd_Deassign(-1,&tape->tapereq,NULL);
            serrno = save_serrno; errno = save_errno;
            CHECK_PROC_ERR(nexttape,NULL,"rtcpd_Mount() error");
        }
	
#ifdef MONITOR /* Monitoring - Loop to count the number of files in the request */
        nb_files = 0;
        last_qty_sent = 0;
        CLIST_ITERATE_BEGIN(nexttape->file,nextfile) {
            nb_files++;
        } CLIST_ITERATE_END(nexttape->file,nextfile);
        current_file = 0;
#endif /* End Monitoring */

        CLIST_ITERATE_BEGIN(nexttape->file,nextfile) {
#ifdef MONITOR
            current_file++;
#endif  /* MONITOR */
 
            mode = nexttape->tapereq.mode;
            /*
             * Leading I/O thread must always check if clients wishes
             * to append more work. Trailing I/O thread must always
             * wait for leading I/O thread to do this check in case
             * they have reached the same file request.
             */
            if ( mode == WRITE_DISABLE ) {
                rtcp_log(LOG_INFO,"tapeIOthread() check with client for more work. Use socket %d\n",
                    client_socket);
                rc = rtcpd_checkMoreWork(&client_socket, nexttape, nextfile);
                CHECK_PROC_ERR(NULL,nextfile,"rtcpd_checkMoreWork() error");
                if ( rc == 1 ) break;
            } else {
                rtcp_log(LOG_INFO,"tapeIOthread() end of filereqs. Wait for diskIO to check for more work\n");
                rc = rtcpd_waitMoreWork(nextfile);
                CHECK_PROC_ERR(NULL,nextfile,"rtcpd_waitMoreWork() error");
                if ( rc == 1 ) break;
            }

            if ( mode == WRITE_DISABLE ) nextfile->filereq.err.severity =
                nextfile->filereq.err.severity & ~RTCP_LOCAL_RETRY;
            /*
             * Handle file section number for multivolume requests. The
             * file section number is 1 for all files on first volume. Only
             * the last (tape) file can span over several volumes and in this
             * case its file section number is incremented for each new volume.
             * The tape file sequence number must be the same as for the
             * last file on previous volume (Ctape does not store this
             * information).
             */
            if ( nexttape == tape ) nextfile->tape_fsec = 1;
            else {
                nextfile->tape_fsec = nexttape->prev->file->tape_fsec+1;
                nextfile->filereq.startsize = prevfile->tapebytes_sofar; 
                nextfile->filereq.tape_fseq = prevfile->filereq.tape_fseq;
                /*
                 * If several disk files are concatenated into one single,
                 * volume spanning, tapefile we have to start with the 
                 * file request structure for the current disk file (which
                 * is already partially copied). We simply skip over the
                 * the file requests for all disk files which were copied
                 * to the previous volume.
                 */
                if ( nextfile->filereq.disk_fseq<prevfile->filereq.disk_fseq ) 
                    nextfile->filereq.proc_status = RTCP_UNREACHABLE;
                if ( prevfile->eovflag != 1 ) {
                    /* 
                     * More volumes than needed have been specified. This
                     * not an error so we just mark the request as ended.
                     */
                    nextfile->filereq.proc_status = RTCP_FINISHED;
                    nextfile->filereq.cprc = prevfile->filereq.cprc;
                    rc = tellClient(&client_socket,NULL,nextfile,0);
                    CHECK_PROC_ERR(NULL,nextfile,"tellClient() error");
                }
            }
            /*
             * If we are concatenating to tape all involved file
             * requests are done in a single iteration. This has to
             * be so to avoid partial blocks in the middle of tape
             * files. A further complication is the concatenation of
             * disk files into a single tape file which is spanning over
             * several volumes: if we have switched tape we obviously
             * need to enter the branch to position the tape and continue
             * the copy....
             */
            rtcp_log(LOG_DEBUG,"tapeIOthread(): file iteration: fseq=%d,%d concat=0x%x, previous file proc_stat=%d\n",
                     nextfile->filereq.tape_fseq,nextfile->filereq.disk_fseq,
                     nextfile->filereq.concat,prevfile->filereq.proc_status);
            if ( (nextfile->filereq.proc_status < RTCP_FINISHED) &&
                 ((nexttape->tapereq.mode == WRITE_DISABLE) ||
                  ((nexttape->tapereq.mode == WRITE_ENABLE) &&
                   (nextfile->filereq.concat & NOCONCAT) != 0) ||
                  (nextfile->tape_fsec != prevfile->tape_fsec)) ) { 
                /*
                 * Position the volume
                 */
                TP_STATUS(RTCP_PS_POSITION);
                rc = rtcpd_Position(nexttape,nextfile);
                TP_STATUS(RTCP_PS_NOBLOCKING);
                CHECK_PROC_ERR(NULL,nextfile,"rtcpd_Position() error");

                /*
                 * Check for tape EOD if we are concatenating rest of 
                 * tape to last disk file. If so, we must wake up the
                 * disk IO thread from waiting for next data buffer.
                 * 6/9/2000:  we allow the same mechanism for deferred stagin
                 *            requests with NOCONCAT_TO_EOD in the last element.
                 *            This is to allow stagin of a complete tape with
                 *            an unspecified number files.
                 */ 
                if ( (nexttape->tapereq.mode == WRITE_DISABLE) &&
                     (((nextfile->filereq.concat & CONCAT_TO_EOD) != 0) ||
                      ((nextfile->filereq.concat & NOCONCAT_TO_EOD) != 0)) ) { 
                    if ( severity == (RTCP_OK | RTCP_EOD) ) {
                        nextfile->filereq.TEndTransferDisk = 
                           nextfile->filereq.TEndTransferTape = (int)time(NULL);
                        nextfile->filereq.err.severity |=
                           nextfile->prev->filereq.err.severity;
                        (void)Cthread_mutex_lock_ext(databufs[indxp]->lock);
                        databufs[indxp]->flag = BUFFER_FULL;
                        (void)Cthread_cond_broadcast_ext(databufs[indxp]->lock);
                        (void)Cthread_mutex_unlock_ext(databufs[indxp]->lock);
                        (void)rtcpd_BroadcastException();
                        break;
                    } else if ( nextfile->next->next == nexttape->file &&
                                ((nextfile->filereq.concat & NOCONCAT_TO_EOD) == 0 || 
                                  nextfile->filereq.def_alloc > 0) ) {
                        /* 
                         * We will reach end of current tape request in
                         * the next iteration. If we are reading tape to EOD
                         * we must make sure that the disk IO thread starter
                         * does not run out of file requests before us ( = the
                         * tape IO thread). Note that the disk IO thread for
                         * for current tape file normally starts before we
                         * finished positioning. Thus we must look two steps
                         * ahead.  
                         */
                        rc = 0;
                        tmpfile = (file_list_t *)calloc(1,sizeof(file_list_t));
                        if ( tmpfile == NULL ) rc = -1;
                        CHECK_PROC_ERR(NULL,nextfile,"calloc() failed");
                        tmpfile->filereq = nextfile->next->filereq;
                        tmpfile->filereq.tape_fseq++;
                        tmpfile->filereq.proc_status = RTCP_WAITING;
                        tmpfile->tape = nexttape;
                        tmpfile->end_index = -1;
                        CLIST_INSERT(nexttape->file,tmpfile);
                        rtcp_log(LOG_DEBUG,"tapeIOthread() create temporary file element for tape fseq=%d\n",tmpfile->filereq.tape_fseq);
                    }
                }
                /*
                 * Fill in missing information (if any). In case
                 * we are writing and the blocksize info is not
                 * specified by user (i.e. we need to take it from
                 * label) we must to synchronise with the disk IO 
                 * thread before updating the information.
                 */
                BroadcastInfo = FALSE;
                if ( nexttape->tapereq.mode == WRITE_ENABLE &&
                    nextfile->filereq.blocksize <= 0 ) {
                    TP_STATUS(RTCP_PS_WAITMTX);
                    rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
                    TP_STATUS(RTCP_PS_NOBLOCKING);
                    CHECK_PROC_ERR(NULL,nextfile,"Cthread_mutex_lock_ext() error");
                    BroadcastInfo = TRUE;
                }
                TP_STATUS(RTCP_PS_INFO);
                rc = rtcpd_Info(nexttape,nextfile);
                TP_STATUS(RTCP_PS_NOBLOCKING);
                if ( rc == -1 ) {
                    save_errno = errno;
                    save_serrno = serrno;
                    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                    errno = save_errno;
                    serrno = save_serrno;
                }
                CHECK_PROC_ERR(NULL,nextfile,"rtcpd_Info() error");
                if ( BroadcastInfo == TRUE ) {
                    /*
                     * We are concatenating to tape and blocksize was
                     * not specified by user. Either a default or
                     * the blocksize of the tape file to be overwritten
                     * has been returned by Ctape_info(). We must now update
                     * all subsequent file requests within this concatenation
                     * since there will be no call to Ctape_info() for them.
                     */
                    CLIST_ITERATE_BEGIN(nextfile,tmpfile) {
                        if ( (tmpfile != nextfile) &&
                             (tmpfile->filereq.concat & NOCONCAT) != 0 ) break;
                        tmpfile->filereq.blocksize = nextfile->filereq.blocksize;
                        tmpfile->filereq.recordlength = nextfile->filereq.recordlength;
                        tmpfile->filereq.proc_status = RTCP_POSITIONED;
                        tmpfile->filereq.TStartPosition = nextfile->filereq.TStartPosition;
                        tmpfile->filereq.TEndPosition = nextfile->filereq.TEndPosition;
                    } CLIST_ITERATE_END(nextfile,tmpfile);
                    rc = Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
                    CHECK_PROC_ERR(NULL,nextfile,"Cthread_cond_broadcast_ext() error");
                    rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
                    CHECK_PROC_ERR(NULL,nextfile,"Cthread_mutex_unlock_ext() error");
                }

                nextfile->filereq.proc_status = RTCP_POSITIONED;

                save_rc = rc;
                rc = tellClient(&client_socket,nexttape,NULL,save_rc);
                CHECK_PROC_ERR(nexttape,NULL,"tellClient() error");
                rc = tellClient(&client_socket,NULL,nextfile,save_rc);
                CHECK_PROC_ERR(NULL,nextfile,"tellClient() error");
                if ( mode == WRITE_ENABLE ) {
                    TP_STATUS(RTCP_PS_STAGEUPDC);
                    rc = rtcpd_stageupdc(nexttape,nextfile);
                    TP_STATUS(RTCP_PS_NOBLOCKING);
                    CHECK_PROC_ERR(NULL,nextfile,"rtcpd_stageupdc() error");
                }

                TP_STATUS(RTCP_PS_WAITMTX);
                rc = rtcpd_SignalFilePositioned(nexttape,nextfile);
                TP_STATUS(RTCP_PS_NOBLOCKING);
                CHECK_PROC_ERR(NULL,nextfile,"rtcpd_SignalFilePositioned() error");

                /*
                 * Open the tape file
                 */
                proc_stat.tapeIOstatus.tape_fseq = 
                    nextfile->filereq.tape_fseq;
                TP_STATUS(RTCP_PS_OPEN);
                rc = topen(nexttape,nextfile);
                TP_STATUS(RTCP_PS_NOBLOCKING);
                if ( rc >= 0 ) tape_fd = rc;
                CHECK_PROC_ERR(NULL,nextfile,"topen() error");

#ifdef MONITOR
                /* Reinit time counter for monitor */
                {
                    struct tms tmp_tms;
                    lasttime_monitor_msg_sent = times(&tmp_tms);
                }
#endif

                if ( nexttape->tapereq.mode == WRITE_ENABLE ) {
                    rc = MemoryToTape(tape_fd,&indxp,&firstblk,&diskIOfinished,
                                      nexttape,nextfile);
                    CHECK_PROC_ERR(NULL,nextfile,"MemoryToTape() error");
                } else {
                    rc = TapeToMemory(tape_fd,&indxp,&firstblk,&diskIOfinished,
                                      nexttape,nextfile);
                    CHECK_PROC_ERR(NULL,nextfile,"TapeToMemory() error");
                }
                /*
                 * Tape file is closed in MemoryToTape() or TapeToMemory()
                 */
                tape_fd = -1;
                prevfile = nextfile;

                if ( nextfile->eovflag == 1 ) {
                    /*
                     * File is spanning two volumes. 
                     * Release to unload but keep drive reservation.
                     */
                    if ( nexttape->next == tape ) {
                        /*
                         * There are no more volumes specified!
                         */
                        tmpfile = nextfile;
                        if ( nexttape->tapereq.mode == WRITE_ENABLE ) {
                            /*
                             * Give client correct number of disk bytes
                             * copied. We have to create a temporary structure
                             * to not confuse the disk IO thread.
                             */
                            tmpfile = (file_list_t *)malloc(sizeof(file_list_t));
                            if ( tmpfile == NULL ) tmpfile = nextfile;
                            else *tmpfile = *nextfile;
                            tmpfile->filereq.bytes_in = tmpfile->tapebytes_sofar;
                            serrno = ENOSPC;
                        } else {
                            serrno = ETEOV;
                        }
                        severity = RTCP_FAILED | RTCP_ENDVOL;
                        rtcpd_AppendClientMsg(NULL,tmpfile,RT124,
                                (mode == WRITE_ENABLE ? "CPDSKTP" : "CPTPDSK"));
                        rtcpd_SetReqStatus(NULL,tmpfile,serrno,severity);
                        rtcpd_BroadcastException();
                        if ( mode == WRITE_ENABLE ) {
                            (void)tellClient(&client_socket,NULL,tmpfile,-1);
                            TP_STATUS(RTCP_PS_STAGEUPDC);
                            rc = rtcpd_stageupdc(nexttape,tmpfile);
                            TP_STATUS(RTCP_PS_NOBLOCKING);
                            rtcpd_SetProcError(severity);
                            (void)rtcp_WriteAccountRecord(client,nexttape,tmpfile,RTCPEMSG);
                            if ( tmpfile != nextfile ) free(tmpfile);
                        }
                        (void)WaitDiskIO();
                        (void)rtcpd_FreeBuffers();
                        TP_STATUS(RTCP_PS_RELEASE);
                        rtcpd_Release(nexttape,NULL);
                        TP_STATUS(RTCP_PS_NOBLOCKING);
                        (void)tellClient(&client_socket,NULL,NULL,0);
                        (void) rtcp_CloseConnection(&client_socket);
                        return((void *)&failure);
                    } else {
                        rtcp_log(LOG_DEBUG,"tapeIOthread() file %d spanns volumes %s:%s\n",
                                 nextfile->filereq.tape_fseq,
                                 nexttape->tapereq.vid,
                                 nexttape->next->tapereq.vid);
                    }
                }

                /*
                 * Keep track of total size stored with this file 
                 * request element. If we concatenate on tape the 
                 * subsequent file request elements involved in the 
                 * concatenation have been implicitly processed 
                 * with this file request.
                 */
                totsz = nextfile->tapebytes_sofar;

                /*
                 * When concatenate on disk, make sure that the
                 * maxsize and maxnbrec are taken into account. We
                 * also check that the maxsizes are set consistently
                 * between the file requests. If not, the error is
                 * logged and the maxsize is corrected to the first
                 * specified value.
                 */
                if ( (nexttape->tapereq.mode == WRITE_DISABLE) &&
                     ((nextfile->next->filereq.concat & CONCAT) != 0 ||
                      (nextfile->filereq.concat &  CONCAT_TO_EOD) != 0) ) {
                    if ( nextfile->filereq.maxsize > 0 ) {
                        if ( nextfile->filereq.maxsize !=
                            nextfile->next->filereq.maxsize ) {
                            rtcp_log(LOG_DEBUG,"tapeIOthread() inconsistent maxsize for concat to disk. Repaired.\n");
                            nextfile->next->filereq.maxsize = 
                                nextfile->filereq.maxsize;
                        }
                        if ( nextfile->tapebytes_sofar <
                             nextfile->filereq.maxsize ) 
                             nextfile->next->filereq.startsize = 
                                 nextfile->tapebytes_sofar;
                        else
                             nextfile->next->filereq.startsize = 
                                 nextfile->filereq.maxsize;
                    }
                }
            } /* !FINISHED && NOCONCAT */
            if ( nexttape->tapereq.mode == WRITE_ENABLE ) {
                bytes_in = nextfile->filereq.bytes_in;
                if ( (nextfile->filereq.concat & CONCAT) != 0 &&
                     (nextfile != prevfile)  ) {
                    /*
                     * When concatenating to tape all disk file
                     * requests are clumped together and treated
                     * in a single call to MemoryToTape(). This means
                     * that the subsequent requests are not passed
                     * to topen() and tclose() and hence the sizes, start
                     * and end transfer time for each individual file
                     * is unknown. We simply set it to the total (all
                     * concatenated files together) start and end.
                     * The test nextfile != prevfile assures that a new
                     * file section (volume spanning) that starts in the
                     * middle of a concat (to tape) chain is not overwritten.
                     */
                    nextfile->filereq.tape_fseq = 
                        nextfile->prev->filereq.tape_fseq;
                    nextfile->filereq.TStartTransferTape = 
                        nextfile->prev->filereq.TStartTransferTape;
                    nextfile->filereq.TEndTransferTape = 
                        nextfile->prev->filereq.TEndTransferTape;
                    nextfile->filereq.TStartPosition = 
                        nextfile->prev->filereq.TStartPosition;
                    nextfile->filereq.TEndPosition = 
                        nextfile->prev->filereq.TEndPosition;
                    nextfile->filereq.nbrecs = 
                        nextfile->prev->filereq.nbrecs;
                    nextfile->filereq.host_bytes = 
                        nextfile->prev->filereq.host_bytes;
                    nextfile->filereq.bytes_out = 
                        nextfile->prev->filereq.bytes_out;
                    strcpy(nextfile->filereq.ifce,nextfile->prev->filereq.ifce);
                    /*
                     * bytes_in contains the value for each disk file. 
                     */ 
                }
                /*
                 * Decrement the total (concatenated) size and check
                 * if it is consistent with sum of the individual
                 * file sizes. This may happen if the max number
                 * of records limit was specified by the user. In
                 * this case we must make sure the copied file size
                 * is updated.
                 */
                if ( totsz > nextfile->filereq.bytes_in ) {
                    totsz -= nextfile->filereq.bytes_in;
                    nextfile->eovflag = prevfile->eovflag;
                    prevfile = nextfile;
                } else {
                    bytes_in = totsz;
                    /*
                     * For a disk file spanning over two volumes we cannot
                     * change bytes_in in the file structure since the disk
                     * IO thread still depends on it.
                     */
                    if ( (nextfile->filereq.concat & VOLUME_SPANNING) == 0 )
                        nextfile->filereq.bytes_in = totsz;
                    totsz = 0;
                    nextfile->eovflag = prevfile->eovflag;
                    if ( bytes_in > 0 ) prevfile = nextfile;
                }


                if ( bytes_in == nextfile->filereq.bytes_in ||
                     nextfile->eovflag == 0 )
                    nextfile->filereq.proc_status = RTCP_FINISHED;
                else if ( bytes_in > 0 )
                    nextfile->filereq.proc_status = RTCP_EOV_HIT;
                else 
                    nextfile->filereq.proc_status = RTCP_UNREACHABLE;

                if ( bytes_in>0 ) {
                    p = u64tostr(bytes_in,u64buf,0);
                    p = strchr(u64buf,' ');
                    if ( p != NULL ) p = '\0';
                    rtcp_log(LOG_INFO,
                       "network interface for data transfer (%s bytes) is %s\n",
                       u64buf,nextfile->filereq.ifce);
                }
                rc = tellClient(&client_socket,NULL,nextfile,0);
                CHECK_PROC_ERR(NULL,nextfile,"tellClient() error");
                TP_STATUS(RTCP_PS_STAGEUPDC);
                rc = rtcpd_stageupdc(nexttape,nextfile);
                TP_STATUS(RTCP_PS_NOBLOCKING);
                CHECK_PROC_ERR(NULL,nextfile,"rtcpd_stageupdc() error");
                (void)rtcp_WriteAccountRecord(client,nexttape,nextfile,RTCPPRC);
            } /* if ( nexttape->tapereq.mode == WRITE_ENABLE ) */
        } CLIST_ITERATE_END(nexttape->file,nextfile);
        if ( nexttape->next != tape ) {
            TP_STATUS(RTCP_PS_RELEASE);
            rc = rtcpd_Release(nexttape,nexttape->file);
            TP_STATUS(RTCP_PS_NOBLOCKING);
            CHECK_PROC_ERR(nexttape,NULL,"rtcpd_Release() error");
        }
    } CLIST_ITERATE_END(tape,nexttape);

    /*
     * Wait for disk IO to finish before releasing the drive. This is to
     * prevent slow disk IO from piling up with too many large rtcopy 
     * on a single server. Also, one of the disk I/O may have hit an ENOSPC.
     */
    rc = WaitDiskIO();
    CHECK_PROC_ERR(nexttape->prev,NULL,"WaitDiskIO() error");
    TP_STATUS(RTCP_PS_RELEASE);
    /*
     * Release the databuffers before release so that there
     * immediately is enough memory for next tape request using
     * this drive.
     */
    (void)rtcpd_FreeBuffers();
    (void)rtcpd_Release(nexttape->prev,NULL);
    TP_STATUS(RTCP_PS_NOBLOCKING);
    TapeIOfinished();
    (void)tellClient(&client_socket,NULL,NULL,0);
    (void) rtcp_CloseConnection(&client_socket);
    return((void *)&success);
}

int rtcpd_StartTapeIO(rtcpClientInfo_t *client, tape_list_t *tape) {
    int rc;
    thread_arg_t *tharg;

    if ( client == NULL || tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    tharg = (thread_arg_t *)malloc(sizeof(thread_arg_t));
    if ( tharg == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_StartTapeIO() malloc(): %s\n",
            sstrerror(errno));
        return(-1);
    }

    /*
     * Open a separate connection to client
     */
    tharg->client_socket = INVALID_SOCKET;
    rc = rtcpd_ConnectToClient(&tharg->client_socket,
                               client->clienthost,
                               &client->clientport);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_StartTapeIO() rtcp_ConnectToClient(%s,%d): %s\n",
                 client->clienthost,client->clientport,sstrerror(serrno));
        return(-1);
    }

    tharg->tape = tape;
    tharg->client = client;

    rc = Cthread_create(tapeIOthread,(void *)tharg);
    if ( rc == -1 )
        rtcp_log(LOG_ERR,"rtcpd_StartTapeIO() Cthread_create(): %s\n",
            sstrerror(serrno));
    else 
        proc_cntl.tapeIOthreadID = rc;

    return(rc);
}

int rtcpd_WaitTapeIO(int *status) {
    int rc,*_status;

    rtcp_log(LOG_DEBUG,"rtcpd_WaitTapeIO() waiting for tape I/O thread\n");

    _status = NULL;
    WaitToJoin = TRUE;
    rc = Cthread_join(proc_cntl.tapeIOthreadID,&_status);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_WaitTapeIO() Cthread_joint(): %s\n",
            sstrerror(serrno));
    } else {
        if ( status != NULL && _status != NULL ) *status = *_status;
    }
    return(rc);
}


#ifdef MONITOR
/*
 * Method added for monitoring purpose.
 * It uses the Cmonit_api to send a UDP message to the monitoring daemon
 */
int rtcpd_sendMonitoring(u_signed64 transfered, u_signed64 total, int forceSend) {
  
  pid_t pid;
  struct tms tmp_tms;
  clock_t tm = times(&tmp_tms);			       

 

  if ( forceSend ||  
       (tm - lasttime_monitor_msg_sent) >  (CMONIT_FTRANSFER_SENDMSG_PERIOD * clktck)) {

    /* Getting the JID, that is the pid of the process. 
       Under linux this is trickier as every thread has its pid */

#if defined(linux)
    pid = getpgrp();
#else
    pid = getpid();
#endif

    if ((tm - lasttime_monitor_msg_sent) != 0) {

      Cmonit_send_transfer_info(pid, transfered, total, (transfered - last_qty_sent) * clktck
				/ (tm - lasttime_monitor_msg_sent), current_file, nb_files);
    } else {
 
      rtcp_log(LOG_DEBUG, "MONITOR - Denominator is 0, no message sent\n");

    }

    lasttime_monitor_msg_sent = tm;
    last_qty_sent = transfered;
  }
  return(0);
} 
#endif
