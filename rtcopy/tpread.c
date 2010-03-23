/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * tpread.c - tpread/tpwrite command line commands
 */

#if defined(_WIN32)
#include <winsock2.h>
#endif /* _WIN32 */
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <signal.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Ctape_api.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <serrno.h>

extern int tpread_command;
static int AbortFlag = 0;

extern int rtcp_InitLog(char *, FILE *, FILE *, SOCKET *);

int CntlC_handler(int sig) {
    (void)sig;
    AbortFlag = 1;
    return(0);
}

int main(int argc, char *argv[]) {
    tape_list_t *tape, *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    char errtxt[10*1024];
    int rc, save_serrno, retval, dont_change_srv;

    rtcp_InitLog(errtxt,NULL,stderr,NULL);
    initlog(argv[0],LOG_INFO,"");
    tpread_command = TRUE;
    tape = NULL;
    rc = rtcpc_BuildReq(&tape,argc,argv);

    retval = 0;
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"%s rtcpc_BuildReq(): %s\n",argv[0],sstrerror(serrno));
        retval = USERR;
    } else {
        /*
         * If user specified a tape server we cannot override that in the
         * retry loop.
         */ 
        dont_change_srv = 0;
        if ( tape != NULL && *tape->tapereq.server != '\0' ) dont_change_srv = 1;

        signal(SIGINT,(void (*)(int))CntlC_handler);

        /*
         * Retry loop to break out of
         */
        for (;;) {
            /**
             * Avoid a segmentation fault when calling rtcpc(tape)
             */
            if(tape == NULL) {
                rtcp_log(LOG_ERR,"Cannot call rtcpc(NULL)\n");
                exit(-1);
            }

            rc = rtcpc(tape);
            save_serrno = serrno;
            if ( AbortFlag != 0 ) break;
            if ( rc == -1 ) {
                if ( rtcpc_CheckRetry(tape) == TRUE ) {
                    if ( dont_change_srv == 0 ) 
                        rtcp_log(LOG_INFO,"Re-selecting another tape server\n");
                    else rtcp_log(LOG_INFO,"Re-select another tape drive on %s\n",
                                  tape->tapereq.server);
                    CLIST_ITERATE_BEGIN(tape,tl) {
                        *tl->tapereq.unit = '\0';
                        if ( dont_change_srv == 0 ) *tl->tapereq.server = '\0'; 
                    } CLIST_ITERATE_END(tape,tl);
                    if ( save_serrno == ETVBSY ) sleep(60);
                    continue;
                } else {
                    rtcp_log(LOG_DEBUG,"%s rtcpc(): %s\n",argv[0],
                             sstrerror(serrno));
                    break;
                }
            } else break;
        }
    }

    CLIST_ITERATE_BEGIN(tape,tl) {
        tapereq = &tl->tapereq;
        dumpTapeReq(tl);
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            dumpFileReq(fl);
        } CLIST_ITERATE_END(tl->file,fl);
    } CLIST_ITERATE_END(tape,tl);

    if ( retval == 0 && AbortFlag == 0 ) {
        (void)rtcp_RetvalSHIFT(tape,NULL,&retval);
    } else {
        rc = 0;
        if ( retval == 0 ) retval = USERR;
    }
    if ( rc == -1 ) retval = UNERR;
    switch (retval) {
    case 0:
        rtcp_log(LOG_INFO,"command successful\n");
        break;
    case RSLCT:
        rtcp_log(LOG_INFO,"Re-selecting another tape server\n");
        break;
    case BLKSKPD:
        rtcp_log(LOG_INFO,"command partially successful since blocks were skipped\n");
        break;
    case TPE_LSZ:
        rtcp_log(LOG_INFO,"command partially successful: blocks skipped and size limited by -s option\n");
        break;
    case MNYPARY:
        rtcp_log(LOG_INFO,"command partially successful: blocks skipped or too many errors on tape\n");
        break;
    case LIMBYSZ:
        rtcp_log(LOG_INFO,"command successful\n");
        break;
    default:
        rtcp_log(LOG_INFO,"command failed\n\n") ;
        break;
    }

    return(retval);
}
