/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

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
#include <rfio_api.h>
#include <rfio_errno.h>
#include <Cthread_api.h>
#include <vdqm_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#if defined(RTCP_SERVER)
#include <rtcp_server.h>
#else  /* RTCP_SERVER */
#include <rtcp_api.h>
#endif /* RTCP_SERVER */
#include <Ctape_api.h>
#include <serrno.h>
int rtcp_CheckReqStructures _PROTO((SOCKET *,
                                          rtcpClientInfo_t *,
                                          tape_list_t *));

#if defined(RTCP_SERVER)
#define SET_REQUEST_ERR(X,Z) {\
    (X)->err.severity = (Z); \
    (X)->err.errorcode = serrno; \
    rtcpd_AppendClientMsg(tape,file,errmsgtxt); \
    if ( ((X)->err.severity & RTCP_FAILED) ) rc = -1;}
#else /* RTCP_SERVER */
#define SET_REQUEST_ERR(X,Z) {\
    (X)->err.severity = (Z); \
    (X)->err.errorcode = serrno; \
    rtcp_log(LOG_ERR,"%s\n",errmsgtxt); \
    if ( ((X)->err.severity & RTCP_FAILED) ) rc = -1;}
#endif /* RTCP_SERVER */

#define CMD(X) ((X)==WRITE_ENABLE ? "CPDSKTP":"CPTPDSK")

static int max_tpretry = MAX_TPRETRY;
static int max_cpretry = MAX_CPRETRY;
extern char *getconfent(char *, char *, int);
#if TMS
extern int rtcp_CallTMS _PROTO((tape_list_t *, char *));
#endif
#if VMGR
extern int rtcp_CallVMGR _PROTO((tape_list_t *, char *));
#endif

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
        if ( tapereq->err.errorcode > 0 ) serrno = tapereq->err.errorcode;
        else serrno = SERTYEXHAUST;
        sprintf(errmsgtxt,"Exiting after %d retries\n",max_tpretry);
        SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    if ( tapereq->err.max_cpretry <= 0 ) {
        if ( tapereq->err.errorcode > 0 ) serrno = tapereq->err.errorcode;
        else serrno = SERTYEXHAUST;
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
            if ( tl != tape && strcmp(tapereq->vid,tl->tapereq.vid) == 0 ) {
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

#define DEBUG_CHKFILE(X) { \
    rtcp_log(LOG_DEBUG,"rtcp_CheckFileReq(%d,%s) check %s %d\n", \
        filereq->tape_fseq,filereq->file_path,#X,filereq->X); \
    }

static int rtcp_CheckFileReq(file_list_t *file) {
    tape_list_t *tape = NULL;
    file_list_t *tmpfile;
    int rc = 0;
    int mode;
    char *p;
    char dir_delim;
    struct stat64 st;
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
     * Ignore placeholder requests used by client to flag
     * the possible intention to later add more requests for
     * this volume
     */
    DEBUG_CHKFILE(proc_status);
    if ( filereq->proc_status == RTCP_REQUEST_MORE_WORK ) return(0);

    /*
     * Retry limits
     */
    DEBUG_CHKFILE(err.max_tpretry);
    if ( filereq->err.max_tpretry <= 0 ) {
        if ( filereq->err.errorcode > 0 ) serrno = filereq->err.errorcode;
        else serrno = SERTYEXHAUST;
        sprintf(errmsgtxt,"Exiting after %d retries\n",max_tpretry);
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    DEBUG_CHKFILE(err.max_cpretry);
    if ( filereq->err.max_cpretry <= 0 ) {
        if ( filereq->err.errorcode > 0 ) serrno = filereq->err.errorcode;
        else serrno = SERTYEXHAUST;
        sprintf(errmsgtxt,"Exiting after %d retries\n",max_cpretry);
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    /*
     * Deferred allocation only valid with stager
     */
    DEBUG_CHKFILE(def_alloc);
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
    DEBUG_CHKFILE(rtcp_err_action);
    if ( filereq->rtcp_err_action == -1 ) 
        filereq->rtcp_err_action = KEEPFILE;

    DEBUG_CHKFILE(tp_err_action);
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
    DEBUG_CHKFILE(convert);
    if ( filereq->convert == -1 ) filereq->convert = ASCCONV;
    if ( (filereq->convert & EBCCONV) == 0 ) filereq->convert |= ASCCONV;
    if ( !VALID_CONVERT(filereq) ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,"INVALID CONVERSION SPECIFIED\n");
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Position method. If not set try to default to TPPOSIT_FSEQ.
     */
    DEBUG_CHKFILE(position_method);
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
    DEBUG_CHKFILE(concat);
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
    DEBUG_CHKFILE(tape_fseq);
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
     * Concat or no concat to EOD is only allowed for tape read
     */
    if ( (mode == WRITE_ENABLE) && 
         ((filereq->concat & (CONCAT_TO_EOD|NOCONCAT_TO_EOD)) != 0) ) {
        sprintf(errmsgtxt,RT143,CMD(mode));
        serrno = EINVAL;
        SET_REQUEST_ERR(filereq, RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * Concat to EOD only allowed for last file element in list
     */
    if ( ((filereq->concat & CONCAT_TO_EOD) != 0) && 
         (file->next != file->tape->file) ) {
        sprintf(errmsgtxt,RT153,CMD(mode));
        serrno = EINVAL;
        SET_REQUEST_ERR(filereq, RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * No concat to EOD is allowed in the last element only if it is
     * deferred stagin request.
     */
    if ( ((filereq->concat & NOCONCAT_TO_EOD) != 0) &&
         (file->next == file->tape->file) &&
         ((*filereq->stageID == '\0') || (filereq->def_alloc == 0)) ) {
        sprintf(errmsgtxt,RT154,CMD(mode));
        serrno = EINVAL;
        SET_REQUEST_ERR(filereq, RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

    /*
     * If concatenate to EOD: insert a dummy file list element
     * so that the diskIOthread waits for the tapeIOthread to
     * signal EOD. 
     * 6/9/2000:  we allow the same mechanism for deferred stagin
     *            requests with NOCONCAT_TO_EOD in the last element.
     *            This is to allow stagin of a complete tape with
     *            an unspecified number files. To facilitate the logic
     *            we allow for NOCONCAT_TO_EOD *only* in the last
     *            element in this case. 
     */
    if ( (mode == WRITE_DISABLE) && 
         ((((filereq->concat & CONCAT_TO_EOD) != 0) &&
           ((file->prev == file) ||
            (file->prev->filereq.concat & CONCAT_TO_EOD) == 0)) ||
          (((filereq->concat & NOCONCAT_TO_EOD) != 0) &&
           (filereq->def_alloc > 0) && (file->next == file->tape->file) &&
           ((file->prev == file) ||
            (file->prev->filereq.concat & NOCONCAT_TO_EOD) == 0))) ) {
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
    DEBUG_CHKFILE(recfm);
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
    DEBUG_CHKFILE(blocksize);
    if ( filereq->blocksize == 0 ) {
        serrno = EINVAL;
        sprintf(errmsgtxt,RT104,CMD(mode));
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
    /*
     * For concatenation to tape, make sure to repeate blocksize
     * since there will be no new tppos
     */
    if ( mode == WRITE_ENABLE && filereq->concat == CONCAT ) {
        if ( filereq->blocksize < 0 ) filereq->blocksize = file->prev->filereq.blocksize;
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
    }
    /*
     * Retention date. If not set we put it to zero.
     */
    DEBUG_CHKFILE(retention);
    if ( filereq->retention < 0 ) filereq->retention = 0;

    /*
     * Disk file name must have been specified
     */
    DEBUG_CHKFILE(file_path);
    if ( *filereq->file_path == '\0' ) {
        serrno = EINVAL;
        if ( *file->prev->filereq.file_path == '\0' )
            sprintf(errmsgtxt,RT107,CMD(mode));
        else
            sprintf(errmsgtxt,RT127,CMD(mode));
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }

#if defined(NS_ROOT)
    /*
     * Make sure that disk file names is not a CASTOR HSM file
     */
    if ( strncmp(filereq->file_path,NS_ROOT,strlen(NS_ROOT)) == 0 ) {
        sprintf(errmsgtxt,RT152,CMD(mode));
        SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
        if ( rc == -1 ) return(rc);
    }
#endif /* NS_ROOT */

    /*
     * Tape file checks flag. Just set a default if not set.
     */
    DEBUG_CHKFILE(check_fid);
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
        rtcp_log(LOG_DEBUG,"rtcp_CheckFileReq(%d,%s) (tpwrite) stat64() remote file\n",
            filereq->tape_fseq,filereq->file_path);
        rfio_errno = serrno = 0;
        rc = rfio_mstat64(filereq->file_path,&st);
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
        rtcp_log(LOG_DEBUG,"rtcp_CheckFileReq(%d,%s) st_mode=0%o\n",
            filereq->tape_fseq,filereq->file_path,st.st_mode);
        if ( ((st.st_mode) & S_IFMT) == S_IFDIR ) {
            serrno = EISDIR;
            sprintf(errmsgtxt,RT110,CMD(mode),sstrerror(serrno));
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }
        /*
         * Zero size?
         */
        rtcp_log(LOG_DEBUG,"rtcp_CheckFileReq(%d,%s) st_size=%d\n",
            filereq->tape_fseq,filereq->file_path,(int)st.st_size);
        if ( st.st_size == 0 ) {
            sprintf(errmsgtxt,RT121,CMD(mode));
            serrno = EINVAL;
            SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
            if ( rc == -1 ) return(rc);
        }

        filereq->bytes_in = (u_signed64)st.st_size;

        DEBUG_CHKFILE(offset);
        if ( filereq->offset > 0 ) {
             if ( filereq->bytes_in <= filereq->offset ) {
                 sprintf(errmsgtxt,RT121,CMD(mode));
                 serrno = EINVAL;
                 SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
                 if ( rc == -1 ) return(rc);
             } else filereq->bytes_in -= filereq->offset;
        }
        DEBUG_CHKFILE(maxsize);
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
        rtcp_log(LOG_DEBUG,"rtcp_CheckFileReq(%d,%s) (tpread) stat64() remote file\n",
            filereq->tape_fseq,filereq->file_path);
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
            rc = rfio_mstat64(filereq->file_path,&st);
            if ( rc != -1 ) rtcp_log(LOG_DEBUG,"rtcp_CheckFileReq(%d,%s) st_mode=0%o\n",
                filereq->tape_fseq,filereq->file_path,st.st_mode);
            if ( rc != -1 && (((st.st_mode) & S_IFMT) == S_IFDIR) ) {
                serrno = EISDIR;
                sprintf(errmsgtxt,RT110,CMD(mode),sstrerror(serrno));
                SET_REQUEST_ERR(filereq,RTCP_USERR | RTCP_FAILED);
                if ( rc == -1 ) return(rc);
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
                rc = rfio_mstat64(filereq->file_path,&st);
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
    rtcp_log(LOG_DEBUG,"rtcp_CheckFileReq(%d,%s) all checks finished, return %d\n",
        filereq->tape_fseq,filereq->file_path,rc);
    return(rc);
}

#if defined(RTCP_SERVER)
/*
 * Externalised file request check for the server. Needed
 * when receiving request updates upon RTCP_REQUEST_MORE_WORK
 * processing status
 */
int rtcpd_CheckFileReq(file_list_t *file) {
    rtcpFileRequest_t *filereq;
    int rc = 0;

    if ( file == NULL ) return(-1);
    filereq = &file->filereq;
    if ( filereq->proc_status != RTCP_FINISHED ) {
        if ( filereq->err.max_tpretry == -1 )
            filereq->err.max_tpretry = max_tpretry;
        if ( filereq->err.max_cpretry == -1 )
            filereq->err.max_cpretry = max_cpretry;
        rc = rtcp_CheckFileReq(file);
    }
    return(rc);
}
#endif /* RTCP_SERVER */


int rtcp_CheckReq(SOCKET *client_socket, 
                  rtcpClientInfo_t *client, 
                  tape_list_t *tape) {
    int rc = 0;
    int save_serrno;
    int nb_filereqs = 0;
    int tot_nb_filereqs = 0;
    tape_list_t *tl;
    file_list_t *fl, *file;
    char *p;
    char errmsgtxt[CA_MAXLINELEN+1];
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( rtcp_CheckReqStructures(client_socket,client,tape) == -1 ) return(-1);

#if VMGR
    /* Call VMGR */
    if ((rc = rtcp_CallVMGR(tape,NULL)) != 0) {
            rtcp_log(LOG_ERR,"rtcp_CheckReq(): rtcp_CallVMGR reported an error!\n");
            return (-1);
    }
#endif

    if ( (p = getenv("RTCOPYD_MAX_TPRETRY")) != NULL )
        max_tpretry = atoi(p);
    else if ( (p = getconfent("RTCOPYD","MAX_TPRETRY",0)) != NULL )
        max_tpretry = atoi(p);

    if ( (p = getenv("RTCOPYD_MAX_CPRETRY")) != NULL )
        max_cpretry = atoi(p);
    else if ( (p = getconfent("RTCOPYD","MAX_CPRETRY",0)) != NULL )
        max_cpretry = atoi(p);

    CLIST_ITERATE_BEGIN(tape,tl) {
        tapereq = &tl->tapereq;
        if ( tapereq->err.max_tpretry == -1 )
            tapereq->err.max_tpretry = max_tpretry;
        if ( tapereq->err.max_cpretry == -1 )
            tapereq->err.max_cpretry = max_cpretry;
        rc = rtcp_CheckTapeReq(tl);
        if ( rc == -1 ) {
#if defined(RTCP_SERVER)
            tellClient(client_socket,tl,NULL,rc);
            (void)rtcp_WriteAccountRecord(client,tl,tl->file,RTCPEMSG);
#endif /* RTCP_SERVER */
            break;
        }

        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            tot_nb_filereqs++;
            if ( filereq->proc_status != RTCP_FINISHED ) {
                nb_filereqs++;
                if ( filereq->err.max_tpretry == -1 )
                    filereq->err.max_tpretry = max_tpretry;
                if ( filereq->err.max_cpretry == -1 )
                    filereq->err.max_cpretry = max_cpretry;
                rc = rtcp_CheckFileReq(fl);
                if ( rc == -1 ) {
#if defined(RTCP_SERVER)
                    tellClient(client_socket,NULL,fl,rc);
                    (void)rtcp_WriteAccountRecord(client,tl,fl,RTCPEMSG);
#endif /* RTCP_SERVER */
                }
                if ( rc == -1 ) break;
            }
        } CLIST_ITERATE_END(tl->file,fl);
        if ( tot_nb_filereqs > 0 && nb_filereqs == 0 ) {
            rtcp_log(LOG_ERR,"rtcp_CheckReq(): All filerequests already finished. Nothing to do!\n");
            sprintf(errmsgtxt,RT127,CMD(tapereq->mode));
            serrno = SEINTERNAL;
            file = NULL;
            SET_REQUEST_ERR(tapereq,RTCP_USERR | RTCP_FAILED);
#if defined(RTCP_SERVER)
            tellClient(client_socket,tape,NULL,rc);
            (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPEMSG);
#endif /* RTCP_SERVER */
        }
        if ( rc == -1 ) break;
    } CLIST_ITERATE_END(tape,tl);
    if ( rfio_end() < 0 && rc != -1 ) {
        save_serrno = (errno > 0 ? errno : serrno);
        tapereq = &tape->tapereq;
        rtcp_log(LOG_ERR,"rtcp_CheckReq() rfio_end(): %s\n",
                 sstrerror(save_serrno));
        sprintf(errmsgtxt,RT136,CMD(tapereq->mode));
        serrno = save_serrno;
        file = NULL;
        SET_REQUEST_ERR(tapereq,RTCP_SYERR | RTCP_FAILED);
#if defined(RTCP_SERVER)
        tellClient(client_socket,tape,NULL,rc);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPEMSG);
#endif /* RTCP_SERVER */
    }
    return(rc);
}

int rtcp_CheckReqStructures(SOCKET *client_socket,
                            rtcpClientInfo_t *client,
                            tape_list_t *tape) {
    int rc;
    tape_list_t *tl;
    file_list_t *file, *fl;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    char errmsgtxt[CA_MAXLINELEN+1];

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    CLIST_ITERATE_BEGIN(tape,tl) {
        tapereq = &tl->tapereq;
        file = NULL;
        if ( tl->next == NULL || tl->prev == NULL ) {
            serrno = ERTNOTCLIST;
            tapereq->tprc = -1;
            strcpy(errmsgtxt,sstrerror(serrno));
            SET_REQUEST_ERR(tapereq,RTCP_FAILED | RTCP_USERR);
#if defined(RTCP_SERVER)
            if ( client_socket != NULL && client != NULL ) {
                tellClient(client_socket,tl,NULL,rc);
                (void)rtcp_WriteAccountRecord(client,tl,tl->file,RTCPEMSG);
            }
#endif /* RTCP_SERVER */
            return(-1);
        }
        if ( !RTCP_TAPEREQ_OK(tapereq) ) {
            serrno = ERTBADREQ;
            tapereq->tprc = -1;
            strcpy(errmsgtxt,sstrerror(serrno));
            SET_REQUEST_ERR(tapereq,RTCP_FAILED | RTCP_USERR);
#if defined(RTCP_SERVER)
            if ( client_socket != NULL && client != NULL ) {
                tellClient(client_socket,tl,NULL,rc);
                (void)rtcp_WriteAccountRecord(client,tl,tl->file,RTCPEMSG);
            }
#endif /* RTCP_SERVER */
            return(-1);
        }
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            file = fl;
            filereq = &fl->filereq;
            if ( fl->next == NULL || fl->prev == NULL ) {
                serrno = ERTNOTCLIST;
                filereq->cprc = -1;
                strcpy(errmsgtxt,sstrerror(serrno));
                SET_REQUEST_ERR(filereq,RTCP_FAILED | RTCP_USERR);
#if defined(RTCP_SERVER)
                if ( client_socket != NULL && client != NULL ) {
                    tellClient(client_socket,NULL,fl,rc);
                    (void)rtcp_WriteAccountRecord(client,tl,fl,RTCPEMSG);
                }
#endif /* RTCP_SERVER */
                return(-1);
            }
            if ( !RTCP_FILEREQ_OK(filereq)) {
                serrno = ERTBADREQ;
                filereq->cprc = -1;
                strcpy(errmsgtxt,sstrerror(serrno));
                SET_REQUEST_ERR(filereq,RTCP_FAILED | RTCP_USERR);
#if defined(RTCP_SERVER)
                if ( client_socket != NULL && client != NULL ) {
                    tellClient(client_socket,NULL,fl,rc);
                    (void)rtcp_WriteAccountRecord(client,tl,fl,RTCPEMSG);
                }
#endif /* RTCP_SERVER */
                return(-1);
            }
        } CLIST_ITERATE_END(tl->file,fl);
    } CLIST_ITERATE_END(tape,tl);

    return(0);
}
