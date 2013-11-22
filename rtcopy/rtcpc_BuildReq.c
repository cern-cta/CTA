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

#define DUMPSTR(Y,X) {if ( *X != '\0' ) rtcp_log(LOG_DEBUG,"%s%s: %s\n",Y,#X,X);}
#define DUMPINT(Y,X) {if ( X != -1 ) rtcp_log(LOG_DEBUG,"%s%s: %d\n",Y,#X,X);}
#define DUMPULONG(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: %lu\n",Y,#X,X);}
#define DUMPBLKID(Y,X) {rtcp_log(LOG_DEBUG,"%s%s: %d:%d:%d:%d\n",Y,#X,(int)X[0],(int)X[1],(int)X[2],(int)X[3]);}
#define DUMPUUID(Y,X) {char *__p; rtcp_log(LOG_DEBUG,"%s%s: %s\n",Y,#X,((__p = CuuidToString(X)) == NULL ? "(null)" : __p));if ( __p != NULL ) free(__p);} 
#define DUMPHEX(Y,X) {if ( X != -1 ) rtcp_log(LOG_DEBUG,"%s%s: 0x%x\n",Y,#X,X);}
#define DUMPI64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: %llu\n",Y,#X,(u_signed64)X);}
#define DUMPX64(Y,X) {if ( X > 0 ) rtcp_log(LOG_DEBUG,"%s%s: 0x%llx\n",Y,#X,(u_signed64)X);}
