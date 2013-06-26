/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpc_BuildReq.c - build the tape and file request list from command
 */


#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Ctape_api.h>
#include <Ctape.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <serrno.h>

extern char *optarg;
extern int optind;

void rtcpc_InitReqStruct(rtcpTapeRequest_t *tapereq,
                         rtcpFileRequest_t *filereq) {
    if ( tapereq != NULL ) {
        memset(tapereq,'\0',sizeof(rtcpTapeRequest_t));
        tapereq->err.max_tpretry = -1;
        tapereq->err.max_cpretry = -1;
        tapereq->err.severity = RTCP_OK;
    }
    if ( filereq != NULL ) {
        memset(filereq,'\0',sizeof(rtcpFileRequest_t));
        filereq->VolReqID = -1;
        filereq->jobID = -1;
        filereq->stageSubreqID = -1;
        filereq->position_method = -1;
        filereq->tape_fseq = -1;
        filereq->disk_fseq = -1;
        filereq->blocksize = -1;
        filereq->recordlength = -1;
        filereq->retention = -1;
        filereq->def_alloc = -1;
        filereq->rtcp_err_action = -1;
        filereq->tp_err_action = -1;
        filereq->convert = -1;
        filereq->check_fid = -1;
        filereq->concat = -1;
        filereq->err.max_tpretry = -1;
        filereq->err.max_cpretry = -1;
        filereq->err.severity = RTCP_OK;
    }
    return;
}

static int newTapeList(tape_list_t **tape, tape_list_t **newtape,
                       int mode) {
    tape_list_t *tl;
    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    tl = (tape_list_t *)calloc(1,sizeof(tape_list_t));
    if ( tl == NULL ) return(-1);
    rtcpc_InitReqStruct(&tl->tapereq,NULL);
    tl->tapereq.mode = mode;
    if ( *tape != NULL ) {
        CLIST_INSERT((*tape)->prev,tl);
    } else {
        CLIST_INSERT(*tape,tl);
    }
    if ( newtape != NULL ) *newtape = tl;
    return(0);
}

int rtcp_NewTapeList(tape_list_t **tape, tape_list_t **newtape,
                              int mode) {
    return(newTapeList(tape,newtape,mode));
}

static int newFileList(tape_list_t **tape, file_list_t **newfile,
                       int mode) {
    file_list_t *fl;
    rtcpFileRequest_t *filereq;
    int rc;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,mode);
        if ( rc == -1 ) return(-1);
    }
    fl = (file_list_t *)calloc(1,sizeof(file_list_t));
    if ( fl == NULL ) return(-1);
    filereq = &fl->filereq;
    rtcpc_InitReqStruct(NULL,filereq);

    /*
     * Insert at end of file request list
     */
    CLIST_INSERT((*tape)->file,fl);

    fl->tape = *tape;
    if ( newfile != NULL ) *newfile = fl;
    return(0);
}

int rtcp_NewFileList(tape_list_t **tape, file_list_t **newfile,
                              int mode) {
    return(newFileList(tape,newfile,mode));
}


#define DMPTP_INT_OPT(X,Y) { \
    if ( (X) < 0 ) { \
        X = strtol(optarg,&dp,10); \
        if ( dp == NULL || *dp != '\0' ) { \
            rtcp_log(LOG_ERR, TP006, #Y); \
            errflg++; \
        } \
    } else { \
        rtcp_log(LOG_ERR, TP018, #Y); \
        errflg++; \
    }}


#define DMPTP_STR_OPT(X,Y) { \
    if ( *(X) == '\0' ) { \
        if ( strlen(optarg) < sizeof((X)) ) strcpy((X),optarg); \
        else { \
            rtcp_log(LOG_ERR, TP006, #Y); \
            errflg++; \
        } \
    } else { \
        rtcp_log(LOG_ERR, TP018, #Y); \
        errflg++; \
    }}

int rtcpc_InitDumpTapeReq(rtcpDumpTapeRequest_t *dump) {
    if ( dump == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    memset(dump,'\0',sizeof(rtcpDumpTapeRequest_t));
    dump->maxbyte = -1;
    dump->blocksize = -1;
    dump->convert = -1;
    dump->tp_err_action = -1;
    dump->startfile = -1;
    dump->maxfile = -1;
    dump->fromblock = -1;
    dump->toblock = -1; 
    return(0);
}

int rtcpc_BuildDumpTapeReq(tape_list_t **tape,
                           int argc, char *argv[]) {
    rtcpDumpTapeRequest_t *dumpreq;
    rtcpTapeRequest_t *tapereq;
    int rc,c;
    char *p, *dp;
    int errflg = 0;
    extern int getopt();

    if ( tape == NULL || argc < 0 || argv == NULL ) {
        rtcp_log(LOG_ERR,"rtcpc_BuildDumpTapeReq() called with NULL args\n");
        serrno = EINVAL;
        return(-1);
    }
    if ( *tape == NULL ) {
        rc = newTapeList(tape,NULL,WRITE_DISABLE);
        if ( rc == -1 ) return(-1);
    }
    tapereq = &(*tape)->tapereq;
    dumpreq = &(*tape)->dumpreq;

    rc = rtcpc_InitDumpTapeReq(dumpreq);
    if ( rc == -1 ) return(rc);

    while ( (c = getopt(argc,argv,"B:b:C:d:E:F:g:N:q:S:T:V:v:")) != EOF) {
        switch (c) {
        case 'B':
            DMPTP_INT_OPT(dumpreq->maxbyte,-B);
            break;
        case 'b':
            DMPTP_INT_OPT(dumpreq->blocksize,-b);
            break;
        case 'C':
            if ( dumpreq->convert < 0 ) {
                if (strcmp (optarg, "ascii") == 0 ) dumpreq->convert = ASCCONV;
                else if ( strcmp (optarg, "ebcdic") == 0 ) 
                    dumpreq->convert = EBCCONV;
                else {
                    rtcp_log(LOG_ERR, TP006, "-C");
                    errflg++;
                }
            } else {
                rtcp_log(LOG_ERR, TP018, "-C");
                errflg++;
            }
            break;
        case 'd':
            DMPTP_STR_OPT(tapereq->density,-d);
            break;
        case 'E':
            if ( strcmp (optarg, "ignoreeoi") == 0 ) 
                dumpreq->tp_err_action = IGNOREEOI;
            else {
                rtcp_log(LOG_ERR, TP006, "-E");
                errflg++;
            }
            break;
        case 'F':
            DMPTP_INT_OPT(dumpreq->maxfile,-F);
            break;
        case 'g':
            DMPTP_STR_OPT(tapereq->dgn,-g);
            break;
        case 'N':
            if ( dumpreq->fromblock < 0 ) {
                if ( (p = strchr (optarg, ',')) != NULL ) {
                    *p++ = '\0';
                    dumpreq->fromblock = strtol(optarg, &dp, 10);
                    if ( dp == NULL || *dp != '\0' ) {
                        rtcp_log(LOG_ERR, TP006, "-N");
                        errflg++;
                    }
                } else {
                    p = optarg;
                    dumpreq->fromblock = 1;
                }
                dumpreq->toblock = strtol(p, &dp, 10);
                if ( dp == NULL || *dp != '\0' ) {
                    rtcp_log(LOG_ERR, TP006, "-N");
                    errflg++;
                }
            }
            break;
        case 'q':
            DMPTP_INT_OPT(dumpreq->startfile,-q);
            break;
        case 'S':
            DMPTP_STR_OPT(tapereq->server,-S);
            break;
        case 'V':
            DMPTP_STR_OPT(tapereq->vid,-V);
            break;
        case 'v':
            DMPTP_STR_OPT(tapereq->vsn,-v);
            break;
        case '?':
            errflg++;
            break;
        }
    }

    if ( *tapereq->vid == '\0' && *tapereq->vsn == '\0' ) {
        rtcp_log(LOG_ERR,TP031);
        errflg++;
    }
    if ( errflg > 0 ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( strcmp(tapereq->dgn,"CT1") == 0 ) strcpy(tapereq->dgn,"CART");
    return(0);
}

#define DUMPSTR(Y,X) {if ( *X != '\0' ) rtcp_log(LOG_DEBUG,"%s%s: %s\n",Y,#X,X);}
#define DUMPINT(Y,X) {if ( X != -1 ) rtcp_log(LOG_DEBUG,"%s%s: %d\n",Y,#X,X);}
#define DUMPULONG(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: %lu\n",Y,#X,X);}
#define DUMPBLKID(Y,X) {rtcp_log(LOG_DEBUG,"%s%s: %d:%d:%d:%d\n",Y,#X,(int)X[0],(int)X[1],(int)X[2],(int)X[3]);}
#define DUMPUUID(Y,X) {char *__p; rtcp_log(LOG_DEBUG,"%s%s: %s\n",Y,#X,((__p = CuuidToString(X)) == NULL ? "(null)" : __p));if ( __p != NULL ) free(__p);} 
#define DUMPHEX(Y,X) {if ( X != -1 ) rtcp_log(LOG_DEBUG,"%s%s: 0x%x\n",Y,#X,X);}
#define DUMPI64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: %llu\n",Y,#X,(u_signed64)X);}
#define DUMPX64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: 0x%llx\n",Y,#X,(u_signed64)X);}
static char *CuuidToString(Cuuid_t *uuid) {
    return(rtcp_voidToString((void *)uuid,sizeof(Cuuid_t)));
}

int dumpTapeReq(tape_list_t *tl) {
    rtcpTapeRequest_t *tapereq;
    char indent[] = " ";

    if (tl == NULL ) return(-1);
    tapereq = &tl->tapereq;

    rtcp_log(LOG_DEBUG,"\n%s---->Tape request\n",indent);
    DUMPSTR(indent,tapereq->vid);
    DUMPSTR(indent,tapereq->vsn);
    DUMPSTR(indent,tapereq->label);
    DUMPSTR(indent,tapereq->dgn);
    DUMPSTR(indent,tapereq->devtype);
    DUMPSTR(indent,tapereq->density);
    DUMPSTR(indent,tapereq->server);
    DUMPSTR(indent,tapereq->unit);
    DUMPINT(indent,tapereq->VolReqID);
    DUMPINT(indent,tapereq->jobID);
    DUMPINT(indent,tapereq->mode);
    DUMPINT(indent,tapereq->start_file);
    DUMPINT(indent,tapereq->end_file);
    DUMPINT(indent,tapereq->side);
    DUMPINT(indent,tapereq->tprc);
    DUMPINT(indent,tapereq->TStartRequest);
    DUMPINT(indent,tapereq->TEndRequest);
    DUMPINT(indent,tapereq->TStartRtcpd);
    DUMPINT(indent,tapereq->TStartMount);
    DUMPINT(indent,tapereq->TEndMount);
    DUMPINT(indent,tapereq->TStartUnmount);
    DUMPINT(indent,tapereq->TEndUnmount);
    DUMPUUID(indent,&(tapereq->rtcpReqId));

    DUMPSTR(indent,tapereq->err.errmsgtxt);
    DUMPHEX(indent,tapereq->err.severity);
    DUMPINT(indent,tapereq->err.errorcode);
    DUMPINT(indent,tapereq->err.max_tpretry);
    DUMPINT(indent,tapereq->err.max_cpretry);

    return(0);
}
int dumpFileReq(file_list_t *fl) {
    rtcpFileRequest_t *filereq;
    char indent[] = "    ";

    if ( fl == NULL ) return(-1);
    filereq = &fl->filereq;

    rtcp_log(LOG_DEBUG,"\n%s---->File request\n",indent);
    DUMPSTR(indent,filereq->file_path);
    DUMPSTR(indent,filereq->tape_path);
    DUMPSTR(indent,filereq->recfm);
    DUMPSTR(indent,filereq->fid);
    DUMPSTR(indent,filereq->ifce);
    DUMPSTR(indent,filereq->stageID);
    DUMPINT(indent,filereq->stageSubreqID);
    DUMPINT(indent,filereq->VolReqID);
    DUMPINT(indent,filereq->jobID);
    DUMPINT(indent,filereq->position_method);
    DUMPINT(indent,filereq->tape_fseq);
    DUMPINT(indent,filereq->disk_fseq);
    DUMPINT(indent,filereq->blocksize);
    DUMPINT(indent,filereq->recordlength);
    DUMPINT(indent,filereq->retention);
    DUMPINT(indent,filereq->def_alloc);
    DUMPINT(indent,filereq->rtcp_err_action);
    DUMPINT(indent,filereq->tp_err_action);
    DUMPINT(indent,filereq->convert);
    DUMPINT(indent,filereq->check_fid);
    DUMPINT(indent,filereq->concat);

    DUMPINT(indent,filereq->proc_status);
    DUMPINT(indent,filereq->cprc);
    DUMPINT(indent,filereq->TStartPosition);
    DUMPINT(indent,filereq->TEndPosition);
    DUMPINT(indent,filereq->TStartTransferDisk);
    DUMPINT(indent,filereq->TEndTransferDisk);
    DUMPINT(indent,filereq->TStartTransferTape);
    DUMPINT(indent,filereq->TEndTransferTape);
    DUMPBLKID(indent,filereq->blockid);

    DUMPI64(indent,filereq->bytes_in);
    DUMPI64(indent,filereq->bytes_out);
    DUMPI64(indent,filereq->host_bytes);

    DUMPI64(indent,filereq->maxnbrec);
    DUMPI64(indent,filereq->maxsize);

    DUMPI64(indent,filereq->startsize);

    DUMPSTR(indent,filereq->castorSegAttr.nameServerHostName);
    DUMPSTR(indent,filereq->castorSegAttr.segmCksumAlgorithm);
    DUMPULONG(indent,filereq->castorSegAttr.segmCksum);
    DUMPI64(indent,filereq->castorSegAttr.castorFileId);

    DUMPUUID(indent,&(filereq->stgReqId));

    DUMPSTR(indent,filereq->err.errmsgtxt);
    DUMPHEX(indent,filereq->err.severity);
    DUMPINT(indent,filereq->err.errorcode);
    DUMPINT(indent,filereq->err.max_tpretry);
    DUMPINT(indent,filereq->err.max_cpretry);

    return(0);
}
