/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcp_CheckReq.c,v $ $Revision: 1.2 $ $Date: 1999/12/09 13:32:08 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_CheckReq.c - Check RTCOPY request and set defaults
 */

#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
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
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
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
    rtcpd_AppendClientMsg(tape,file,errmsgtxt); \
    (X)->err.severity = (Z); \
    (X)->err.errorcode = serrno; \
    if ( ((X)->err.severity & RTCP_FAILED) ) rc = -1;}

static int max_tpretry = MAX_TPRETRY;
static int max_cpretry = MAX_CPRETRY;
extern char *getconfent(char *, char *, int);

static int rtcp_CheckTapeReq(tape_list_t *tape) {
    file_list_t *file = NULL;
    int rc = 0;
    rtcpTapeRequest_t *tapereq;
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
        strcpy(errmsgtxt,"vsn or vid must be specified");
        SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    if ( *tapereq->vid == '\0' ) strcpy(tapereq->vid,tapereq->vsn);


    return(rc);
}

static int rtcp_CheckFileReq(file_list_t *file) {
    tape_list_t *tape = NULL;
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
    filereq = &file->filereq;
    mode = file->tape->tapereq.mode;

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
        strcpy(errmsgtxt,"The 'A' option set to DEFERRED is valid with the stager only !");
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
        strcpy(errmsgtxt,"INVALID ERROR ACTION SPECIFIED");
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Record conversion
     */
    if ( filereq->convert == -1 ) filereq->convert = ASCCONV;
    if ( !VALID_CONVERT(filereq) ) {
        serrno = EINVAL;
        strcpy(errmsgtxt,"INVALID CONVERSION SPECIFIED");
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Concatenation of files on disk or tape. If not set,
     * try to set it...
     */
    if ( filereq->concat == -1 ) {
        rtcp_log(LOG_INFO,"Concatenation switch not set. Trying to set it\n");
        if ( mode == WRITE_DISABLE && file->prev != file ) {
            /*
             * Tape read
             */
            if ( strcmp(file->prev->filereq.file_path,filereq->file_path) == 0 )
                filereq->concat = CONCAT;
            else
                filereq->concat = NOCONCAT;
        } else if ( file->prev == file ) filereq->concat = NOCONCAT;
        else {
            /*
             * Tape write
             */
            if ( file->prev->filereq.tape_fseq == filereq->tape_fseq )
                filereq->concat = CONCAT;
            else
                filereq->concat = NOCONCAT;
        }
    }
    if ( !VALID_CONCAT(filereq) ) {
        serrno = EINVAL;
        strcpy(errmsgtxt,"INVALID SETTING FOR CONCATENATION");
        SET_REQUEST_ERR(filereq, RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Record format.
     */
    if ( *filereq->recfm == '\0' ) strcpy(filereq->recfm,"U");
    if ( strcmp(filereq->recfm,"F") != 0 &&
         strcmp(filereq->recfm,"FB") != 0 &&
         strcmp(filereq->recfm,"FBS") != 0 &&
         strcmp(filereq->recfm,"FS") != 0 &&
         strcmp(filereq->recfm,"U") != 0 ) {
        serrno = EINVAL;
        strcpy(errmsgtxt,"INVALID FORMAT SPECIFIED");
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Blocksize and record length
     * Client is not allowed to specify 0.
     */
    if ( filereq->blocksize == 0 ) {
        serrno = EINVAL;
        strcpy(errmsgtxt,"Block size cannot be equal to zero");
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
            sprintf(errmsgtxt,"record length (%d) can't be greater than block size (%d)",
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
            strcpy(errmsgtxt,"disk file pathnames must be specified\n");
        else
            strcpy(errmsgtxt,"incorrect number of filenames specified\n");
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
     *  File section number. Just set a default (1) if not set.
     */
    if ( filereq->tape_fsec <= 0 ) filereq->tape_fsec = 1;

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
        rc = rfio_stat(filereq->file_path,&st);
        if ( rc == -1 ) {
            if ( rfio_errno != 0 ) serrno = rfio_errno;
            else if ( serrno == 0 ) serrno = errno;
            strcpy(errmsgtxt,sstrerror(serrno));
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }
        /*
         * Is it a director?
         */
        if ( ((st.st_mode) & S_IFMT) == S_IFDIR ) {
            sprintf(errmsgtxt,"File %s is a directory !",filereq->file_path);
            serrno = EISDIR;
            SET_REQUEST_ERR(filereq,RTCP_OK);
        }
        /*
         * Zero size?
         */
        if ( st.st_size == 0 ) {
            sprintf(errmsgtxt,"File %s is empty !",filereq->file_path);
            serrno = EINVAL;
            SET_REQUEST_ERR(filereq,RTCP_OK);
        }

        filereq->bytes_in = (u_signed64)st.st_size;
        if ( filereq->maxsize > 0 ) {
            /*
             * Calculate start size if we are concatenating
             */
            if ( filereq->maxsize > 0 ) {
                if ( file->prev != file && 
                    file->prev->filereq.tape_fseq == filereq->tape_fseq ) {
                    filereq->startsize = file->prev->filereq.startsize +
                        file->prev->filereq.bytes_in;
                }
                if ( filereq->maxsize <= filereq->startsize ) {
                    filereq->startsize = filereq->maxsize;
                    filereq->bytes_in = 0;
                    serrno = EFBIG;
                    SET_REQUEST_ERR(filereq,RTCP_OK | RTCP_LIMBYSZ);
                }

                if ( filereq->bytes_in + filereq->startsize > filereq->maxsize ) {
                    filereq->bytes_in = filereq->maxsize - filereq->startsize;
                    serrno = EFBIG;
                    SET_REQUEST_ERR(filereq,RTCP_OK | RTCP_LIMBYSZ);
                }
            }
        }
    } else {
        /*
         * Tape read:
         * rfiod cannot get token to write to AFS based files -> check.
         * Should we check that disk file doesn't exist...? Wasnt'
         * done before so skip that for the time being. However,
         * if it exists we must check that it is not a directory!.
         */
        if ( strstr(filereq->file_path,":/afs") != NULL ) {
            sprintf(errmsgtxt,RT145,"CPTPDSK");
            serrno = EACCES;
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }
        rfio_errno = serrno = 0;
        rc = rfio_stat(filereq->file_path,&st);
        if ( rc != -1 && (((st.st_mode) & S_IFMT) == S_IFDIR) ) {
            sprintf(errmsgtxt,"File %s is a directory !",filereq->file_path);
            serrno = EISDIR;
            SET_REQUEST_ERR(filereq,RTCP_OK);
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
            rc = rfio_stat(filereq->file_path,&st);
            if ( rc == -1 ) {
                if ( rfio_errno != 0 ) serrno = rfio_errno;
                else if ( serrno == 0 ) serrno = errno;
                sprintf(errmsgtxt,"%s: %s",filereq->file_path,sstrerror(serrno));
                if ( p != NULL ) *p = dir_delim;
                SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
                if ( rc == -1 ) return(rc);
            }
            /*
             * Couldn't find S_ISDIR() macro under NT. 
             */
            if ( !(((st.st_mode) & S_IFMT) == S_IFDIR) ) {
                sprintf(errmsgtxt,"directory %s does not exist",
                    filereq->file_path);
                if ( p != NULL ) *p = dir_delim;
                serrno = ENOTDIR;
                SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
                if ( rc == -1 ) return(rc);
            }
            if ( p != NULL ) *p = dir_delim;
        }
    }
    return(rc);
}


int rtcp_CheckReq(SOCKET *client_socket, tape_list_t *tape) {
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
            break;
        }

        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            if ( filereq->err.max_tpretry == -1 )
                filereq->err.max_tpretry = max_tpretry;
            if ( filereq->err.max_cpretry == -1 )
                filereq->err.max_cpretry = max_cpretry;
            rc = rtcp_CheckFileReq(fl);
            if ( *(filereq->err.errmsgtxt) != '\0' )
                tellClient(client_socket,NULL,fl,rc);
            if ( rc == -1 ) break;
        } CLIST_ITERATE_END(tl->file,fl);

        if ( rc == -1 ) break;
    } CLIST_ITERATE_END(tape,tl);
    return(rc);
}

