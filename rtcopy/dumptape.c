#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <errno.h>
#include <serrno.h>
#include <osdep.h>
#include <Castor_limits.h>
#include <net.h>
#include <log.h>
#include <Ctape_api.h>
#include <rtcp_api.h>

extern int tpread_command;
static int AbortFlag = 0;

extern int rtcp_InitLog(char *, FILE *, FILE *, SOCKET *);

int CntlC_handler(int sig) {
    AbortFlag = 1;
    return(0);
}


int main(int argc, char *argv[]) {
    tape_list_t *tape = NULL;
    tape_list_t *tl = NULL;
    file_list_t *fl = NULL;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    char errtxt[1024];
    int rc, retval;

    rtcp_InitLog(errtxt,NULL,stderr,NULL);
    initlog(argv[0],LOG_INFO,"");
    tpread_command = TRUE;

    retval = AbortFlag = 0;
    rc = rtcpc_BuildDumpTapeReq(&tape,argc,argv);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpc_BuildDumpTapeReq(): %s\n",sstrerror(serrno));
        retval = -1;
    } else {
        signal(SIGINT,(void (*)(int))CntlC_handler);
        rc = rtcpc(tape);
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
        rc = rtcp_RetvalSHIFT(tape,NULL,&retval);
    } else {
        rc = 0;
        if ( retval == 0 ) retval = USERR;
    }
    if ( retval == 0 ) rtcp_log(LOG_INFO,"command successful\n");
    else rtcp_log(LOG_INFO,"command failed\n");
    return(retval);
}
