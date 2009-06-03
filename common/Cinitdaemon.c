/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

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

#if defined(linux) || defined(__APPLE__)
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
#if defined(linux) || defined(__APPLE__)
        c = setsid();
#else
        c = setpgrp();
#endif
        /* Redirect standard files to /dev/null */
        freopen( "/dev/null", "r", stdin);
        freopen( "/dev/null", "w", stdout);
        freopen( "/dev/null", "w", stderr);

        for (c = 3; c < maxfds; c++)
                close (c);
        if ( wait4child != NULL ) {
                sa.sa_handler = wait4child;
                sa.sa_flags = SA_RESTART;
                sigaction (SIGCHLD, &sa, NULL);
        }
        return (maxfds);
}
#endif
