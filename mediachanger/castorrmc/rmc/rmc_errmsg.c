/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2001-2021 CERN
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

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>

#include "rmc_constants.h"

static char *errbufp = NULL;
static int errbuflen;

/*	rmc_seterrbuf - set receiving buffer for error messages */

void
rmc_seterrbuf(char *buffer,
	      int buflen)
{
	errbufp = buffer;
	errbuflen = buflen;
}

/* rmc_errmsg - send error message to user defined client buffer or to stderr */

int rmc_errmsg(char *func, char *msg, ...)
{
	va_list args;
	char prtbuf[RMC_PRTBUFSZ];
	int save_errno;

	save_errno = errno;
	va_start (args, msg);
	if (func)
		sprintf (prtbuf, "%s: ", func);
	else
		*prtbuf = '\0';
	vsprintf (prtbuf + strlen(prtbuf), msg, args);
	if (errbufp) {
		if ((int)strlen (prtbuf) < errbuflen) {
			strcpy (errbufp, prtbuf);
		} else {
			strncpy (errbufp, prtbuf, errbuflen - 2);
			errbufp[errbuflen-2] = '\n';
			errbufp[errbuflen-1] = '\0';
		}
	} else {
		fprintf (stderr, "%s", prtbuf);
	}
	va_end (args);
	errno = save_errno;
	return (0);
}
