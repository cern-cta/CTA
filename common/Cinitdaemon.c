/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * Cinitdaemon.c - Common routine for CASTOR daemon initialisation
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h> 
#include <errno.h>
#include <osdep.h>
#include <string.h>
#include <serrno.h>

int Cinitdaemon(char *name,
		void (*wait4child) (int))
{
        int c;
        int maxfds;
        struct sigaction sa;

        maxfds = getdtablesize();
        /* Background */
        if ((c = fork()) < 0) {
                fprintf (stderr, "%s: cannot fork: %s\n",name,sstrerror(errno));
                exit(1);
        } else
                if (c > 0) exit (0);
        c = setsid();
        /* Redirect standard files to /dev/null */
        freopen( "/dev/null", "r", stdin);
        freopen( "/dev/null", "w", stdout);
        freopen( "/dev/null", "w", stderr);

        for (c = 3; c < maxfds; c++)
                close (c);
        if ( wait4child != NULL ) {
                memset(&sa, 0, sizeof(sa));
                sa.sa_handler = wait4child;
                sa.sa_flags = SA_RESTART;
                sigaction (SIGCHLD, &sa, NULL);
        }
        return (maxfds);
}
