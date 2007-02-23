/*
 * $Id: rtcpd.c,v 1.3 2007/02/23 09:30:12 sponcec3 Exp $
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
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
#endif /* _WIN32 */
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
extern char *rtcpd_logfile;
extern int use_port;
extern int Debug;
extern int loglevel;
extern int rtcp_InitLog _PROTO((char *, FILE *, FILE *, SOCKET *));
extern int Cinitdaemon _PROTO((char *, void (*)(int)));

static int ChdirWorkdir() {
#if !defined(_WIN32)
    char workdir[] = WORKDIR;
    struct stat st;
    mode_t save_mask;
    int rc;

    /*
     * Check and create if necessary the RTCOPY working directory
     */
    if ( stat(workdir,&st) == -1 ) {
        if ( errno == ENOENT ) {
            save_mask = umask(0);
            rc = mkdir(workdir,S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|
S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH);
            if ( rc == -1 ) rtcp_log(LOG_ERR,"Cannot create directory %s: %s\n",
                                     workdir,sstrerror(errno)); 
            else rtcp_log(LOG_INFO,"Created directory %s\n",workdir);
            umask(save_mask);
        }
    } else if ( !S_ISDIR(st.st_mode) ) {
        rtcp_log(LOG_ERR,"%s should be a directory !\n",workdir);
    }
    if ( chdir(workdir) == -1 ) {
        rtcp_log(LOG_ERR,"chdir(%s): %s\n",workdir,sstrerror(errno));
        return(-1);
    }
#endif /* _WIN32 */
    return(0);
}

int rtcpd_main(struct main_args *main_args) {
    int rc;
    int pid;
    char workdir[] = WORKDIR;
    char tpserver[CA_MAXHOSTNAMELEN+1];
    time_t starttime;
    SOCKET *listen_socket = NULL;
    SOCKET *request_socket = NULL;
    SOCKET accept_socket = INVALID_SOCKET;

#if !defined(_WIN32)
    signal(SIGPIPE,SIG_IGN);
    /*
     * Ignoring SIGXFSZ signals before logging anything
     */   
    signal(SIGXFSZ,SIG_IGN);
#endif /* _WIN32 */

    rtcp_InitLog(NULL,NULL,NULL,NULL);

    request_socket = (SOCKET *)calloc(1,sizeof(SOCKET));
    if ( request_socket == NULL ) {
        rtcp_log(LOG_ERR,"main() calloc(SOCKET): %s\n",sstrerror(errno));
        return(1);
    }

    rc = rtcpd_InitNW(&listen_socket);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"main() rtcpd_InitNW(): %s\n",sstrerror(serrno));
        return(1);
    }

#if defined(CTAPE_DUMMIES)
    rc = Ctape_InitDummy();
#endif /* CTAPE_DUMMIES */
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"main() Ctape_InitDummy(): %s\n",
            sstrerror(serrno));
        return(1);
    }
    /*
     * Go to working directory
     */
    rc = ChdirWorkdir();
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"main() ChdirWorkdir(): %s\n",sstrerror(errno));
        return(1);
    }

    gethostname(tpserver,CA_MAXHOSTNAMELEN);
    starttime = time(NULL);
#if defined(__DATE__) && defined (__TIME__)
    rtcp_log(LOG_INFO,"Rtcopy server generated at %s %s. Started in %s:%s\n",
             __DATE__,__TIME__,tpserver,workdir);
#endif /* __DATE__ && __TIME__ */
 
    rc = 0;
    for (;;) {
        rc = rtcp_Listen(*listen_socket,&accept_socket,-1, RTCP_ACCEPT_FROM_CLIENT);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"main() rtcp_Listen(): %s\n",
                sstrerror(serrno));
            continue;
        }
#if !defined(_WIN32) && !defined(CTAPE_DUMMIES)
        pid = (int)fork();
#else  /* !_WIN32 && !CTAPE_DUMMIES */
        pid = 0;
#endif /* _WIN32 */
        if ( pid == -1 ) {
            rtcp_log(LOG_ERR,"main() failed to fork(): %s\n",
                sstrerror(errno));
            continue;
        }
        if ( pid != 0 ) {
            closesocket(accept_socket);
            continue; /* Parent */
        }

        /*
         * Child
         */
#if !defined(_WIN32) && !defined(CTAPE_DUMMIES)
        signal(SIGPIPE,SIG_IGN);
        /*
         * Ignoring SIGXFSZ signals before logging anything
         */
        signal(SIGXFSZ,SIG_IGN);
        closesocket(*listen_socket);
        free(listen_socket);
        listen_socket = NULL;
#endif /* !_WIN32 && !CTAPE_DUMMIES */
        *request_socket = accept_socket;
        rc = rtcpd_MainCntl(request_socket);
#if !defined(_WIN32) && !defined(CTAPE_DUMMIES)
        break;
#endif /* !_WIN32 && !CTAPE_DUMMIES */
    }
    (void)rtcp_CleanUp(&listen_socket,rc);
    if ( request_socket != NULL ) free(request_socket);
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

    if ( Debug == TRUE ) exit(rtcpd_main(NULL));

#if defined(_WIN32)
/*
 * Windows
 */
#if !defined(CTAPE_DUMMIES)
    if ( Cinitservice("rtcpd",rtcpd_main) == -1 ) exit(1);
#else /* !CTAPE_DUMMIES */
    exit(rtcpd_main(NULL));
#endif /* !CTAPE_DUMMIES */


#else /* _WIN32 */
/*
 * UNIX
 */
#if !defined(CTAPE_DUMMIES)
    if ( Cinitdaemon("rtcpd",SIG_IGN) == -1 ) exit(1);
#endif /* CTAPE_DUMMIES */
    exit(rtcpd_main(NULL));
#endif /* _WIN32 */
}
