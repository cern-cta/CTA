/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "$RCSfile: rtcp_RetvalSHIFT.c,v $ $Revision: 1.6 $ $Date: 2000/03/13 11:29:23 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

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
#endif /* _WIN32 */

#include <errno.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <vdqm_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <Ctape_api.h>
#include <serrno.h>

int rtcp_RetvalSHIFT(tape_list_t *tape, file_list_t *file, int *Retval) {
    tape_list_t *tl;
    file_list_t *fl;
    rtcpErrMsg_t *err;
    int retval, rc;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    err = NULL;
    tl = NULL;
    fl = NULL;
    CLIST_ITERATE_BEGIN(tape,tl) {
        if ( tl->tapereq.tprc != 0 ) {
            err = &(tl->tapereq.err);
            break;
        }
        if ( file == NULL ) {
            CLIST_ITERATE_BEGIN(tl->file,fl) {
                if ( fl->filereq.cprc != 0 ) {
                    err = &(fl->filereq.err);
                    break;
                }
            } CLIST_ITERATE_END(tl->file,fl);
        } else {
            fl = file;
            err = &(fl->filereq.err);
        }
        if ( fl != NULL && fl->filereq.cprc != 0 ) break;
    } CLIST_ITERATE_END(tape,tl);

    if ( err != NULL ) {
        if ( (err->severity & RTCP_FAILED) != 0 ) {
            if ( (err->severity & RTCP_RESELECT_SERV) != 0 ) retval = RSLCT;
            else if ( (err->severity & RTCP_USERR) != 0 ) retval = USERR;
            else if ( (err->severity & RTCP_SYERR) != 0 ) retval = SYERR;
            else if ( (err->severity & RTCP_UNERR) != 0 ) retval = UNERR;
            else if ( (err->severity & RTCP_SEERR) != 0 ) retval = SEERR;
            else if ( (err->severity & RTCP_ENDVOL) != 0 ) retval = ENDVOL;
            else retval = UNERR;
        } else {
            if ( (err->severity & RTCP_BLKSKPD) != 0 ) retval = BLKSKPD;
            else if ( (err->severity & RTCP_TPE_LSZ) != 0 ) retval = TPE_LSZ;
            else if ( (err->severity & RTCP_MNYPARY) != 0 ) retval = MNYPARY;
            else if ( (err->severity & RTCP_LIMBYSZ) != 0 ) retval = LIMBYSZ;
            else if ( (err->severity & RTCP_ENDVOL) != 0 ) retval = ENDVOL;
            else retval = 0;
        }
    } else retval = 0;

    if ( Retval != NULL ) *Retval = retval;
    return(0);
}
