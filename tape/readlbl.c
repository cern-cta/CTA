/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: readlbl.c,v $ $Revision: 1.6 $ $Date: 2000/01/09 17:42:35 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	readlbl - read one possible label record */
/*	return	-errno	in case of error
 *		0	if a 80 characters record was read
 *		1	if the record length was different
 *		2	for EOF
 *		3	for blank tape
 */
#include <errno.h>
#include <sys/types.h>
#include "Ctape.h"
#include "serrno.h"
extern char *sys_errlist[];
readlbl(tapefd, path, lblbuf)
int tapefd;
char *path;
char *lblbuf;
{
	int errcat;
	char func[16];
	char *msgaddr;
	int n;
	int rc;

	ENTRY (readlbl);
	if ((n = read (tapefd, lblbuf, LBLBUFSZ)) < 0) {
#if sgi
		if (errno == ENOSPC) RETURN (3);	/* blank tape */
		if (errno != ENOMEM) {	/* not large block */
#else
#if sun
		if (errno != EINVAL && errno != ENOMEM) {	/* not large block */
#else
#if __alpha && __osf__
		if (errno == ENOSPC) {
			RETURN (3);	/* blank tape */
		} else {
#else
#if _IBMR2
		if (errno != ENOMEM) {	/* not large block */
#else
#if linux
		if (errno != ENOMEM) {	/* not large block */
#else
		{
#endif
#endif
#endif
#endif
#endif
			if (errno == ENOSPC) {
				msgaddr = "";
				rc = ETPARIT;
			} else if (errno == EIO) {
				errcat = gettperror (tapefd, path, &msgaddr);
#if defined(sun) || defined(RS6000PCTA) || defined(ADSTAR) || defined(hpux) || defined(linux)
				if (errcat == ETBLANK) RETURN (3);
#endif
				rc = errcat;
			} else {
				msgaddr = sys_errlist[errno];
				rc = errno;
#if defined(_IBMR2)
				if (errno == EMEDIA) rc = ETPARIT;
#endif
			}
			usrmsg (func, TP042, path, "read", msgaddr);
			RETURN (-rc);
		}
	}
	if (n == 0) {
#if defined(sun) || defined(linux)
		if (gettperror (tapefd, path, &msgaddr) == ETBLANK)
			RETURN (3);	/* blank tape */
#endif
		RETURN (2);	/* tapemark */
	}
	lblbuf[80] = '\0';
	RETURN (0);
}
