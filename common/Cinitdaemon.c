/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cinitdaemon.c,v $ $Revision: 1.7 $ $Date: 2003/10/31 12:48:25 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * Cinitdaemon.c - Common routine for CASTOR daemon initialisation
 */

#if ! defined(_WIN32)
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h> 
#include <errno.h>
#include <osdep.h>
#include <serrno.h>

int DLL_DECL Cinitdaemon(name,wait4child)
char *name;
void (*wait4child) _PROTO((int));
{
        int c;
        int maxfds;
        struct sigaction sa;

#if defined(SOLARIS) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(sgi)
        maxfds = getdtablesize();
#else
        maxfds = _NFILE;
#endif
        /* Background */
        if ((c = fork()) < 0) {
                fprintf (stderr, "%s: cannot fork: %s\n",name,sstrerror(errno));
                exit(1);
        } else
                if (c > 0) exit (0);
#if (defined(__osf__) && defined(__alpha)) || defined(linux)
        c = setsid();
#else
#if hpux
        c = setpgrp3();
#else
        c = setpgrp();
#endif
#endif
        for (c = 0; c < maxfds; c++)
                close (c);
        if ( wait4child != NULL ) {
                sa.sa_handler = wait4child;
                sa.sa_flags = SA_RESTART;
                sigaction (SIGCHLD, &sa, NULL);
        }
        return (maxfds);
}
#endif
