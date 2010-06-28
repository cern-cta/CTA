/*
 * $Id: rtcpd.c,v 1.8 2009/08/18 09:43:01 waldron Exp $
 *
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd.c - Remote Tape Copy Daemon main program.
 */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <errno.h>
#include <patchlevel.h>
#include <Castor_limits.h>
#include <log.h>
#include <unistd.h>
#include <net.h>
#include <osdep.h>
#include <trace.h>
#include <serrno.h>
#include <Cgetopt.h>
#include <Cinit.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include "tplogger_api.h"

extern char *rtcpd_logfile;
extern int use_port;
extern int Debug;
extern int loglevel;
extern int rtcp_InitLog _PROTO((char *, FILE *, FILE *, SOCKET *));
extern int Cinitdaemon _PROTO((char *, void (*)(int)));

char *getconfent();

static int ChdirWorkdir() {
    char *workdir = WORKDIR, *p;
    struct stat st;
    mode_t save_mask;
    int rc;

    if ( ((p = getenv("RTCOPY_WORKDIR")) != NULL) ||
         ((p = getconfent("RTCOPY","WORKDIR",0)) != NULL) ) {
        workdir = strdup(p);
    }
    /*
     * Check and create if necessary the RTCOPY working directory
     */
    if ( stat(workdir,&st) == -1 ) {
        if ( errno == ENOENT ) {
            save_mask = umask(0);
            rc = mkdir(workdir,S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|
S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH);
            if ( rc == -1 ) {
                    rtcp_log(LOG_ERR,"Cannot create directory %s: %s\n",
                                      workdir,sstrerror(errno));
                    tl_rtcpd.tl_log( &tl_rtcpd, 3, 4,
                                     "func"   , TL_MSG_PARAM_STR, "ChdirWorkdir",
                                     "Message", TL_MSG_PARAM_STR, "Cannot create directory",
                                     "Workdir", TL_MSG_PARAM_STR, workdir,
                                     "Error"  , TL_MSG_PARAM_STR, sstrerror(errno) );
            } else {
                    rtcp_log(LOG_INFO,"Created directory %s\n",workdir);
                    tl_rtcpd.tl_log( &tl_rtcpd, 10, 3,
                                     "func"   , TL_MSG_PARAM_STR, "ChdirWorkdir",
                                     "Message", TL_MSG_PARAM_STR, "Created directory",
                                     "Workdir", TL_MSG_PARAM_STR, workdir );
            }
            umask(save_mask);
        }
    } else if ( !S_ISDIR(st.st_mode) ) {
        rtcp_log(LOG_ERR,"%s should be a directory !\n",workdir);
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 3,
                         "func"   , TL_MSG_PARAM_STR, "ChdirWorkdir",
                         "Message", TL_MSG_PARAM_STR, "Should be a directory",
                         "Workdir", TL_MSG_PARAM_STR, workdir );
    }
    if ( chdir(workdir) == -1 ) {
        rtcp_log(LOG_ERR,"chdir(%s): %s\n",workdir,sstrerror(errno));
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 4,
                         "func"   , TL_MSG_PARAM_STR, "ChdirWorkdir",
                         "Message", TL_MSG_PARAM_STR, "chdir",
                         "Workdir", TL_MSG_PARAM_STR, workdir,
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(errno) );
        return(-1);
    }
    return(0);
}

int rtcpd_main() {
    int rc;
    int pid;
    char workdir[] = WORKDIR;
    char tpserver[CA_MAXHOSTNAMELEN+1];
    time_t starttime;
    SOCKET *listen_socket = NULL;
    SOCKET *request_socket = NULL;
    SOCKET accept_socket = INVALID_SOCKET;

    signal(SIGPIPE,SIG_IGN);
    /*
     * Ignoring SIGXFSZ signals before logging anything
     */
    signal(SIGXFSZ,SIG_IGN);

    rtcp_InitLog(NULL,NULL,NULL,NULL);

    /* init the tplogger interface */
    {
            mode_t save_mask;
            char *p;

            save_mask = umask(0);

            p = getconfent ("TAPE", "TPLOGGER", 0);
            if (p && (0 == strcasecmp(p, "SYSLOG"))) {
                    tl_init_handle( &tl_rtcpd, "syslog" );
            } else {
                    tl_init_handle( &tl_rtcpd, "dlf" );
            }
            tl_rtcpd.tl_init( &tl_rtcpd, 1 );

            tl_rtcpd.tl_log( &tl_rtcpd, 9, 2,
                             "func"   , TL_MSG_PARAM_STR, "rtcpd_main",
                             "Message", TL_MSG_PARAM_STR, "Logging initialized" );
            umask(save_mask);
    }
    request_socket = (SOCKET *)calloc(1,sizeof(SOCKET));
    if ( request_socket == NULL ) {
            rtcp_log(LOG_ERR,"main() calloc(SOCKET): %s\n",sstrerror(errno));
            tl_rtcpd.tl_log( &tl_rtcpd, 3, 3,
                             "func"   , TL_MSG_PARAM_STR, "rtcpd_main",
                             "Message", TL_MSG_PARAM_STR, "main() calloc (SOCKET)",
                             "Error"  , TL_MSG_PARAM_STR, sstrerror(errno) );
            return(1);
    }

    rc = rtcpd_InitNW(&listen_socket);
    if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"main() rtcpd_InitNW(): %s\n",sstrerror(serrno));
            tl_rtcpd.tl_log( &tl_rtcpd, 3, 3,
                             "func"   , TL_MSG_PARAM_STR, "rtcpd_main",
                             "Message", TL_MSG_PARAM_STR, "main() rtcpd_InitNW()",
                             "Error"  , TL_MSG_PARAM_STR, sstrerror(serrno) );
            return(1);
    }

#if defined(CTAPE_DUMMIES)
    rc = Ctape_InitDummy();
#endif /* CTAPE_DUMMIES */
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"main() Ctape_InitDummy(): %s\n",
                 sstrerror(serrno));
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 3,
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_main",
                         "Message", TL_MSG_PARAM_STR, "main() Ctape_InitDummy()",
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(serrno) );
        return(1);
    }
    /*
     * Go to working directory
     */
    rc = ChdirWorkdir();
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"main() ChdirWorkdir(): %s\n",sstrerror(errno));
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 3,
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_main",
                         "Message", TL_MSG_PARAM_STR, "main() ChdirWorkdir()",
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(errno) );
        return(1);
    }

    gethostname(tpserver,CA_MAXHOSTNAMELEN);
    starttime = time(NULL);
#if defined(__DATE__) && defined (__TIME__)
    rtcp_log(LOG_INFO,"Rtcopy server generated at %s %s. Started in %s:%s\n",
             __DATE__,__TIME__,tpserver,workdir);
    tl_rtcpd.tl_log( &tl_rtcpd,  11, 5,
                     "Message",  TL_MSG_PARAM_STR, "Rtcopy server generated",
                     "date",     TL_MSG_PARAM_STR, __DATE__,
                     "time",     TL_MSG_PARAM_STR, __TIME__,
                     "tpserver", TL_MSG_PARAM_STR, tpserver,
                     "workdir",  TL_MSG_PARAM_STR, workdir );
#endif /* __DATE__ && __TIME__ */

    rc = 0;
    for (;;) {
        rc = rtcp_Listen(*listen_socket,&accept_socket,-1, RTCP_ACCEPT_FROM_CLIENT);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"main() rtcp_Listen(): %s\n",
                sstrerror(serrno));
            tl_rtcpd.tl_log( &tl_rtcpd, 3, 3,
                             "func"   , TL_MSG_PARAM_STR, "rtcpd_main",
                             "Message", TL_MSG_PARAM_STR, "main() rtcp_Listen()",
                             "Error"  , TL_MSG_PARAM_STR, sstrerror(serrno) );
            continue;
        }

#if !defined(CTAPE_DUMMIES)
        pid = (int)fork();
#else  /* !CTAPE_DUMMIES */
        pid = 0;
#endif /* !CTAPE_DUMMIES */
        if ( pid == -1 ) {
            rtcp_log(LOG_ERR,"main() failed to fork(): %s\n",
                     sstrerror(errno));
            tl_rtcpd.tl_log( &tl_rtcpd, 3, 3,
                             "func"   , TL_MSG_PARAM_STR, "rtcpd_main",
                             "Message", TL_MSG_PARAM_STR, "main() failed to fork()",
                             "Error"  , TL_MSG_PARAM_STR, sstrerror(errno) );
            continue;
        }
        if ( pid != 0 ) {
            closesocket(accept_socket);
            continue; /* Parent */
        }

        /*
         * Child
         */
#if !defined(CTAPE_DUMMIES)
        signal(SIGPIPE,SIG_IGN);
        /*
         * Ignoring SIGXFSZ signals before logging anything
         */
        signal(SIGXFSZ,SIG_IGN);
        closesocket(*listen_socket);
        free(listen_socket);
        listen_socket = NULL;
#endif /* !CTAPE_DUMMIES */
        *request_socket = accept_socket;
        rc = rtcpd_MainCntl(request_socket);
#if !defined(CTAPE_DUMMIES)
        break;
#endif /* !CTAPE_DUMMIES */
    }
    (void)rtcp_CleanUp(&listen_socket,rc);
    if ( request_socket != NULL ) free(request_socket);

    tl_rtcpd.tl_exit( &tl_rtcpd, 0 );

    return(0);
}

int main(int argc, char *argv[]) {
    int c;

    Coptind = 1;
    Copterr = 1;

    while ( (c = Cgetopt(argc, argv, "df:p:")) != -1 ) {
        switch (c) {
        case 'd':
            Debug = TRUE;
            loglevel = LOG_DEBUG;
            NOTRACE;
            break;
        case 'f':
            rtcpd_logfile = strdup(Coptarg);
            if ( rtcpd_logfile == NULL ) {
                fprintf(stderr,"main() strdup(): %s\n",
                    strerror(errno));
                return(1);
            }
            break;
        case 'p':
            use_port = atoi(Coptarg);
            break;
        default:
            break;
        }
    }

    if ( Debug == TRUE ) exit(rtcpd_main());

#if !defined(CTAPE_DUMMIES)
    if ( Cinitdaemon("rtcpd",SIG_IGN) == -1 ) exit(1);
#endif /* CTAPE_DUMMIES */
    exit(rtcpd_main());
}
