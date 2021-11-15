/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2000-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

int Cinitdaemon(const char *const name,
		void (*const wait4child) (int))
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
        if (freopen( "/dev/null", "r", stdin) == NULL) {
                fprintf (stderr, "%s: cannot freeopen stdin: %s\n",name,sstrerror(errno));
                exit(1);
        }
        if (freopen( "/dev/null", "w", stdout) == NULL) {
                fprintf (stderr, "%s: cannot freeopen stdout: %s\n",name,sstrerror(errno));
                exit(1);
        }
        if (freopen( "/dev/null", "w", stderr) == NULL) {
                fprintf (stderr, "%s: cannot freeopen stderr: %s\n",name,sstrerror(errno));
                exit(1);
        }

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
