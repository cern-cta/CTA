/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tpread.c,v $ $Revision: 1.1 $ $Date: 1999/11/29 11:22:09 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * tpread.c - tpread/tpwrite command line commands
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
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <serrno.h>

extern int rtcp_InitLog(char *, FILE *, FILE *);

int main(int argc, char *argv[]) {
    tape_list_t *tape, *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    char errtxt[1024];
    int rc;

    rtcp_InitLog(errtxt,stdout,stderr);
    initlog(argv[0],LOG_INFO,"");
    tape = NULL;
    rc = rtcpc_BuildReq(&tape,argc,argv);

    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"%s rtcpc_BuildReq(): %s\n",argv[0],sstrerror(serrno));
        return(1);
    }

    rc = rtcpc(tape);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"%s rtcpc(): %s\n",argv[0],sstrerror(serrno));
        return(1);
    }

    CLIST_ITERATE_BEGIN(tape,tl) {
        tapereq = &tl->tapereq;
        dumpTapeReq(tl);
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            dumpFileReq(fl);
        } CLIST_ITERATE_END(tl->file,fl);
    } CLIST_ITERATE_END(tape,tl);

    return(0);
}
