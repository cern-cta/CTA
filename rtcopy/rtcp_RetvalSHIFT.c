/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd_RetvalSHIFT.c - RTCOPY routines to calculate tpread/tpwrite command RC
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
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <vdqm_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <Ctape_api.h>
#include <serrno.h>

typedef enum maptype {map_shift,map_castor} maptype_t;

static int rtcp_Retval(tape_list_t *tape, file_list_t *file, 
                       int *Retval, maptype_t what) {
    tape_list_t *tl;
    file_list_t *fl;
    rtcpErrMsg_t *err;
    int retval, incomplete;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    err = NULL;
    tl = NULL;
    fl = NULL;
    incomplete = 0;
    CLIST_ITERATE_BEGIN(tape,tl) {
        if ( tl->tapereq.tprc != 0 ) {
            err = &(tl->tapereq.err);
            rtcp_log(LOG_DEBUG,"rtcp_Retval() tape RC==%d\n",tl->tapereq.tprc);
            break;
        }
        if ( file == NULL ) {
            CLIST_ITERATE_BEGIN(tl->file,fl) {
                if ( fl->filereq.cprc != 0 ) {
                    err = &(fl->filereq.err);
                    rtcp_log(LOG_DEBUG,"rtcp_Retval() file RC=%d\n",
                             fl->filereq.cprc);
                    break;
                }
                if ( fl->filereq.proc_status < RTCP_FINISHED ) incomplete++;
            } CLIST_ITERATE_END(tl->file,fl);
        } else {
            fl = file;
            err = &(fl->filereq.err);
            if ( fl->filereq.proc_status < RTCP_FINISHED ) incomplete++;
        }
        if ( fl != NULL && fl->filereq.cprc != 0 ) break;
    } CLIST_ITERATE_END(tape,tl);

    if ( err != NULL ) {
        if ( (err->severity & (RTCP_FAILED|RTCP_RESELECT_SERV) ) != 0 ) {
            if ( what == map_castor ) retval = err->errorcode;
            else {
                if ( (err->severity & RTCP_RESELECT_SERV) != 0 ) retval = RSLCT;
                else if ( (err->severity & RTCP_USERR) != 0 ) retval = USERR;
                else if ( (err->severity & RTCP_SYERR) != 0 ) retval = SYERR;
                else if ( (err->severity & RTCP_UNERR) != 0 ) retval = UNERR;
                else if ( (err->severity & RTCP_SEERR) != 0 ) retval = SEERR;
                else if ( (err->severity & RTCP_ENDVOL) != 0 ) {
                    if ( tape->tapereq.mode == WRITE_ENABLE ) retval = ENOSPC;
                    else retval = ENDVOL;
                } else retval = UNERR;
            }
        } else {
            if ( (err->severity & RTCP_BLKSKPD) != 0 ) 
                if ( what == map_castor ) retval = ERTBLKSKPD;
                else retval = BLKSKPD;
            else if ( (err->severity & RTCP_TPE_LSZ) != 0 ) 
                if ( what == map_castor ) retval = ERTTPE_LSZ;
                else retval = TPE_LSZ;
            else if ( (err->severity & RTCP_MNYPARY) != 0 ) 
                if ( what == map_castor ) retval = ERTMNYPARY;
                else retval = MNYPARY;
            else if ( (err->severity & RTCP_LIMBYSZ) != 0 ) 
                if ( what == map_castor ) retval = ERTLIMBYSZ;
                else retval = LIMBYSZ;
            else if ( (err->severity & RTCP_ENDVOL) != 0 ) 
                if ( what == map_castor ) retval = err->errorcode;
                else retval = ENDVOL;
            else retval = 0;
        }
        rtcp_log(LOG_DEBUG,"rtcp_Retval() map severity %d to status %d\n",err->severity,retval);
    } else retval = 0;

    if ( retval == 0 && incomplete > 0 ) {
        rtcp_log(LOG_DEBUG,"rtcp_Retval() found %d unprocessed file requests\n",
            incomplete);
        if ( what == map_castor ) retval = SEINTERNAL;
        else retval = UNERR;
    }

    if ( Retval != NULL ) *Retval = retval;
    return(0);
}

int rtcp_RetvalSHIFT(tape_list_t *tape, file_list_t *file, int *Retval) {
    maptype_t what = map_shift;
    return(rtcp_Retval(tape,file,Retval,what));
}

int rtcp_RetvalCASTOR(tape_list_t *tape, file_list_t *file, int *Retval) {
    maptype_t what = map_castor;
    return(rtcp_Retval(tape,file,Retval,what));
}
