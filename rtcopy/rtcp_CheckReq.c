/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcp_CheckReq.c,v $ $Revision: 1.31 $ $Date: 2000/04/13 15:44:18 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_CheckReq.c - Check RTCOPY request and set defaults
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#endif /* _WIN32 */

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
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
#include <vdqm_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <Ctape_api.h>
#include <serrno.h>

#define SET_REQUEST_ERR(X,Z) {\
    (X)->err.severity = (Z); \
    (X)->err.errorcode = serrno; \
    rtcp_log(LOG_ERR,errmsgtxt); \
    rtcpd_AppendClientMsg(tape,file,errmsgtxt); \
    if ( ((X)->err.severity & RTCP_FAILED) ) rc = -1;}

#define CMD(X) ((X)==WRITE_ENABLE ? "CPDSKTP":"CPTPDSK")

static int max_tpretry = MAX_TPRETRY;
static int max_cpretry = MAX_CPRETRY;
extern char *getconfent(char *, char *, int);

static int rtcp_CheckTapeReq(tape_list_t *tape) {
    file_list_t *file = NULL;
    tape_list_t *tl;
    int rc = 0;
    rtcpTapeRequest_t *tapereq;
    rtcpDumpTapeRequest_t *dumpreq = NULL;
    char errmsgtxt[CA_MAXLINELEN+1];

    *errmsgtxt = '\0';
    if ( tape == NULL ) {
        serrno = SEINTERNAL;
        return(rc);
    }

    tapereq = &tape->tapereq;
    /*
     * Retry limits
     */
    if ( tapereq->err.max_tpretry <= 0 ) {
        serrno = SERTYEXHAUST;
        sprintf(errmsgtxt,"Exiting after %d retries\n",max_tpretry);
        SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    if ( tapereq->err.max_cpretry <= 0 ) {
        serrno = SERTYEXHAUST;
        sprintf(errmsgtxt,"Exiting after %d retries\n",max_cpretry);
        SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    /*
     * VID and VSN
     */
    if ( *tapereq->vid == '\0' && *tapereq->vsn == '\0' ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,RT144,CMD(tapereq->mode));
        SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    if ( *tapereq->vid == '\0' ) strcpy(tapereq->vid,tapereq->vsn);

    /*
     * Volume spanning? Make sure VID is unique.
     */
    if ( tape != tape->next ) {
        CLIST_ITERATE_BEGIN(tape,tl) {
            if ( strcmp(tapereq->vid,tl->tapereq.vid) == 0 ) {
                serrno = EINVAL;
                sprintf(errmsgtxt,"VID %s NOT UNIQUE IN VOLUME SPANNING\n",
                        tapereq->vid);
                SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
                if ( rc == -1 ) return(rc);
            }
        } CLIST_ITERATE_END(tape,tl);
    }

    /*
     * Dumptape ? 
     */
    if ( tape->file == NULL ) {
        dumpreq = &tape->dumpreq;
        /*
         * Mode must be WRITE_DISABLE
         */
        if ( tapereq->mode == WRITE_ENABLE ) {
            serrno = EINVAL;
            sprintf(errmsgtxt,"dumptape is invalid with WRITE_ENABLE mode set\n");
            SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }

        if ( dumpreq->tp_err_action < 0 ) dumpreq->tp_err_action = 0;
        if ( dumpreq->convert < 0 ) dumpreq->convert = EBCCONV;
        if ( !VALID_CONVERT(dumpreq) ) {
            serrno = EINVAL;
            sprintf(errmsgtxt,"INVALID CONVERSION SPECIFIED\n");
            SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }
    }

    return(rc);
}

static int rtcp_CheckFileReq(file_list_t *file) {
    tape_list_t *tape = NULL;
    file_list_t *tmpfile;
    int rc = 0;
    int mode;
    const u_signed64 defmaxsize = (u_signed64)2 * (u_signed64)(1024*1024*1024) - 1;
    char *p;
    char dir_delim;
    struct stat st;
    rtcpFileRequest_t *filereq;
    char errmsgtxt[CA_MAXLINELEN+1];

    *errmsgtxt = '\0';
    if ( file == NULL ) {
        serrno = SEINTERNAL;
        return(-1);
    }
    tape = file->tape;
    filereq = &file->filereq;
    mode = tape->tapereq.mode;

    /*
     * Retry limits
     */
    if ( filereq->err.max_tpretry <= 0 ) {
        serrno = SERTYEXHAUST;
        sprintf(errmsgtxt,"Exiting after %d retries\n",max_tpretry);
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    if ( filereq->err.max_cpretry <= 0 ) {
        serrno = SERTYEXHAUST;
        sprintf(errmsgtxt,"Exiting after %d retries\n",max_cpretry);
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    /*
     * Deferred allocation only valid with stager
     */
    if ( filereq->def_alloc == -1 ) filereq->def_alloc = 0;
    if ( filereq->def_alloc != 0 && *filereq->stageID == '\0' ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,"The 'A' option set to DEFERRED is valid with the stager only !\n");
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Error action
     */
    if ( filereq->rtcp_err_action == -1 ) 
        filereq->rtcp_err_action = KEEPFILE;

    if ( filereq->tp_err_action == -1 )
        filereq->tp_err_action = 0;

    if ( !VALID_ERRACT(filereq) ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,"INVALID ERROR ACTION SPECIFIED\n");
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Record conversion
     */
    if ( filereq->convert == -1 ) filereq->convert = ASCCONV;
    if ( !VALID_CONVERT(filereq) ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,"INVALID CONVERSION SPECIFIED\n");
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Position method. If not set try to default to TPPOSIT_FSEQ.
     */
    if ( filereq->position_method != TPPOSIT_FSEQ &&
         filereq->position_method != TPPOSIT_FID &&
         filereq->position_method != TPPOSIT_EOI &&
         filereq->position_method != TPPOSIT_BLKID ) {
        if ( filereq->tape_fseq > 0 ) filereq->position_method = TPPOSIT_FSEQ;
        else {
            serrno = EINVAL;
            sprintf(errmsgtxt,"INVALID POSITION METHOD SPECIFIED\n");
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }
    }

    /*
     * Concatenation of files on disk or tape. If not set,
     * try to set it...
     */
    if ( filereq->concat == -1 ) {
        rtcp_log(LOG_DEBUG,"Concatenation switch not set. Trying to set it\n");
        if ( mode == WRITE_DISABLE && file->prev != file ) {
            /*
             * Tape read
             */
            if ( strcmp(filereq->file_path,".") != 0 &&
                 strcmp(file->prev->filereq.file_path,filereq->file_path) == 0 )
                filereq->concat = CONCAT;
            else
                filereq->concat = NOCONCAT;
        } else if ( file->prev == file ) filereq->concat = NOCONCAT;
        else {
            /*
             * Tape write
             */
            if ( (filereq->position_method == TPPOSIT_FSEQ &&
                  file->prev->filereq.tape_fseq == filereq->tape_fseq) ||
                 (filereq->position_method == TPPOSIT_FID &&
                  strcmp(file->prev->filereq.fid,filereq->fid) == 0) )
                filereq->concat = CONCAT;
            else
                filereq->concat = NOCONCAT;
        }
    }
    /*
     * Tape files must be in sequence on write
     */
    if ( mode == WRITE_ENABLE && file != tape->file && 
         (filereq->concat & NOCONCAT) != 0 &&
         (filereq->position_method == TPPOSIT_FSEQ) &&
        (file->prev->filereq.tape_fseq != filereq->tape_fseq - 1) ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,RT151,CMD(mode));
        SET_REQUEST_ERR(filereq,RTCP_SYERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * If concatenate to EOD: insert a dummy file list element
     * so that the diskIOthread waits for the tapeIOthread to
     * signal EOD.
     */
    if ( (mode == WRITE_DISABLE) && ((filereq->concat & CONCAT_TO_EOD) != 0) &&
         ((file->prev == file) || 
          (file->prev->filereq.concat & CONCAT_TO_EOD) == 0) ) {
        tmpfile = (file_list_t *)calloc(1,sizeof(file_list_t));
        if ( tmpfile == NULL ) {
            serrno = errno;
            sprintf(errmsgtxt,"calloc() failed\n");
            SET_REQUEST_ERR(filereq,RTCP_SYERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }
        tmpfile->filereq = file->filereq;
        tmpfile->filereq.tape_fseq++;
        tmpfile->tape = tape;
        tmpfile->end_index = -1;
        rtcp_log(LOG_DEBUG,"rtcp_CheckFileReq() create temporary file element for tape fseq %d\n",tmpfile->filereq.tape_fseq);
        CLIST_INSERT(tape->file,tmpfile);
    }

    /*
     * Cannot concatenate in single file requests
     */
    if ( (filereq->concat & CONCAT) != 0 && file == file->next ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,"INVALID SETTING FOR CONCATENATION\n");
        SET_REQUEST_ERR(filereq, RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    if ( !VALID_CONCAT(filereq) ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,"INVALID SETTING FOR CONCATENATION\n");
        SET_REQUEST_ERR(filereq, RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Record format.
     */
    if ( *filereq->recfm != '\0' &&
         strcmp(filereq->recfm,"F") != 0 &&
         strcmp(filereq->recfm,"FB") != 0 &&
         strcmp(filereq->recfm,"FBS") != 0 &&
         strcmp(filereq->recfm,"FS") != 0 &&
         strcmp(filereq->recfm,"U") != 0 ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,RT130,CMD(mode));
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * F,-f77 format is valid for tape read mode only! 
     */
    if ( (mode == WRITE_ENABLE) && (*filereq->recfm == 'F') && 
         ((filereq->convert & NOF77CW) != 0) ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,"NOF77CW for F format is not valid for tape write\n");
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    } 

    /*
     * Blocksize and record length
     * Client is not allowed to specify 0.
     */
    if ( filereq->blocksize == 0 ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,RT104,CMD(mode));
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    /*
     * If the client didn't provide any value we put it to zero
     */
    if ( filereq->blocksize < 0 ) filereq->blocksize = 0;
    if ( filereq->recordlength < 0 ) filereq->recordlength = 0;

    if ( filereq->blocksize > 0 && filereq->recordlength > 0 ) {
        if ( filereq->recordlength > filereq->blocksize ) {
            serrno = EINVAL;
            sprintf(errmsgtxt,RT138,CMD(mode),
                filereq->recordlength,filereq->blocksize);
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }
        if ( *filereq->recfm == 'U' ) {
            sprintf(errmsgtxt,"record length (%d) no effect for U format file\n",
                filereq->recordlength);
            SET_REQUEST_ERR(filereq,RTCP_OK);
        }
    }
    /*
     * Retention date. If not set we put it to zero.
     */
    if ( filereq->retention < 0 ) filereq->retention = 0;

    /*
     * Disk file name must have been specified
     */
    if ( *filereq->file_path == '\0' ) {
        serrno = EINVAL;
        if ( *file->prev->filereq.file_path == '\0' )
            sprintf(errmsgtxt,RT107,CMD(mode));
        else
            sprintf(errmsgtxt,RT127,CMD(mode));
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Tape file checks flag. Just set a default if not set.
     */
    if ( filereq->check_fid < 0 ) {
        if ( mode == WRITE_ENABLE ) filereq->check_fid = NEW_FILE;
        else filereq->check_fid = CHECK_FILE;
    }

    /*
     * File sizes, etc...
     */
    if ( mode == WRITE_ENABLE ) {
        /*
         * For tape write:
         * Check that disk file exists and fill in its size. 
         * Check and signal eventual truncation.
         */
        rfio_errno = serrno = 0;
        rc = rfio_mstat(filereq->file_path,&st);
        if ( rc == -1 ) {
            if ( rfio_errno != 0 ) serrno = rfio_errno;
            else if ( serrno == 0 ) serrno = errno;
            sprintf(errmsgtxt,RT110,CMD(mode),sstrerror(serrno));
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }
        /*
         * Is it a director?
         */
        if ( ((st.st_mode) & S_IFMT) == S_IFDIR ) {
            serrno = EISDIR;
            sprintf(errmsgtxt,RT110,CMD(mode),sstrerror(serrno));
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        }
        /*
         * Zero size?
         */
        if ( st.st_size == 0 ) {
            sprintf(errmsgtxt,RT121,CMD(mode));
            serrno = EINVAL;
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        }

        filereq->bytes_in = (u_signed64)st.st_size;
        if ( filereq->offset > 0 ) filereq->bytes_in -= filereq->offset;
        if ( filereq->maxsize > 0 ) {
            /*
             * Calculate start size if we are concatenating
             */
            if ( filereq->maxsize > 0 ) {
                if ( filereq->concat == CONCAT ) {
                    filereq->startsize = file->prev->filereq.startsize +
                        file->prev->filereq.bytes_in;
                }
                if ( filereq->maxsize <= filereq->startsize ) {
                    filereq->startsize = filereq->maxsize;
                    filereq->bytes_in = 0;
                    serrno = ERTLIMBYSZ;
                    SET_REQUEST_ERR(filereq,RTCP_OK | RTCP_LIMBYSZ);
                }

                if ( filereq->bytes_in + filereq->startsize > filereq->maxsize ) {
                    filereq->bytes_in = filereq->maxsize - filereq->startsize;
                    serrno = ERTLIMBYSZ;
                    SET_REQUEST_ERR(filereq,RTCP_OK | RTCP_LIMBYSZ);
                }
            }
        }
    } else {
        /*
         * Tape read:
         * Only accept dot (".") if it is a stager + deferred request.
         * rfiod cannot get token to write to AFS based files -> check.
         * Should we check that disk file doesn't exist...? Wasnt'
         * done before so skip that for the time being. However,
         * if it exists we must check that it is not a directory!.
         */
        if ( strcmp(filereq->file_path,".") == 0 ) {
            if ( *filereq->stageID == '\0' ||
                 filereq->def_alloc == 0 ) {
                sprintf(errmsgtxt,"File = %s only valid for deferred stagein\n",
                        filereq->file_path);
                serrno = EINVAL;
                SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
                if ( rc == -1 ) return(rc);
            } 
        } else {
            if ( strstr(filereq->file_path,":/afs") != NULL ) {
                sprintf(errmsgtxt,RT145,CMD(mode));
                serrno = EACCES;
                SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
                if ( rc == -1 ) return(rc);
            }
            rfio_errno = serrno = 0;
            rc = rfio_mstat(filereq->file_path,&st);
            if ( rc != -1 && (((st.st_mode) & S_IFMT) == S_IFDIR) ) {
                serrno = EISDIR;
                sprintf(errmsgtxt,RT110,CMD(mode),sstrerror(serrno));
                SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
            }
            /*
             * If the file doesn't exist we should at least make sure 
             * that the target directory exists.
             */
            if ( rc == -1 ) {
                p = strrchr(filereq->file_path,'/');
                if ( p == NULL ) p = strrchr(filereq->file_path,'\\');
                if ( p != NULL ) {
                    dir_delim = *p;
                    *p = '\0';
                }
                rc = rfio_mstat(filereq->file_path,&st);
                if ( rc == -1 ) {
                    if ( rfio_errno != 0 ) serrno = rfio_errno;
                    else if ( serrno == 0 ) serrno = errno;
                    sprintf(errmsgtxt,RT110,CMD(mode),sstrerror(serrno));
                    if ( p != NULL ) *p = dir_delim;
                    SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
                    if ( rc == -1 ) return(rc);
                }
                /*
                 * Couldn't find S_ISDIR() macro under NT. 
                 */
                if ( !(((st.st_mode) & S_IFMT) == S_IFDIR) ) {
                    serrno = ENOTDIR;
                    sprintf(errmsgtxt,RT110,CMD(mode),sstrerror(serrno));
                    if ( p != NULL ) *p = dir_delim;
                    SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
                    if ( rc == -1 ) return(rc);
                }
                if ( p != NULL ) *p = dir_delim;
            }
        }
    }
    return(rc);
}


int rtcp_CheckReq(SOCKET *client_socket, 
                  rtcpClientInfo_t *client, 
                  tape_list_t *tape) {
    int rc = 0;
    tape_list_t *tl;
    file_list_t *fl;
    char *p;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    rc = rtcp_CallTMS(tape);

    if ( (p = getconfent("RTCOPYD","MAX_TPRETRY",0)) != NULL )
        max_tpretry = atoi(p);

    if ( (p = getconfent("RTCOPYD","MAX_CPRETRY",0)) != NULL )
        max_cpretry = atoi(p);

    CLIST_ITERATE_BEGIN(tape,tl) {
        tapereq = &tl->tapereq;
        if ( tapereq->err.max_tpretry == -1 )
            tapereq->err.max_tpretry = max_tpretry;
        if ( tapereq->err.max_cpretry == -1 )
            tapereq->err.max_cpretry = max_cpretry;
        rc = rtcp_CheckTapeReq(tl);
        if ( rc == -1 ) {
            tellClient(client_socket,tl,NULL,rc);
            (void)rtcp_WriteAccountRecord(client,tl,tl->file,RTCPEMSG);
            break;
        }

        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            if ( filereq->err.max_tpretry == -1 )
                filereq->err.max_tpretry = max_tpretry;
            if ( filereq->err.max_cpretry == -1 )
                filereq->err.max_cpretry = max_cpretry;
            rc = rtcp_CheckFileReq(fl);
            if ( *(filereq->err.errmsgtxt) != '\0' ) {
                tellClient(client_socket,NULL,fl,rc);
                (void)rtcp_WriteAccountRecord(client,tl,fl,RTCPEMSG);
            }
            if ( rc == -1 ) break;
        } CLIST_ITERATE_END(tl->file,fl);

        if ( rc == -1 ) break;
    } CLIST_ITERATE_END(tape,tl);
    rfio_end();
    return(rc);
}

