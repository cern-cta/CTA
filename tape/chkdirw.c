/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: chkdirw.c,v $ $Revision: 1.3 $ $Date: 1999/10/13 14:37:53 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	chkdirw - extract directory name from full pathname
 *		and check if writable.
 *
 *	return	-errno	in case of error
 *		0	if writable
 */
#include <errno.h>
#if defined(_WIN32)
#define W_OK 2
#else
#include <unistd.h>
#endif
chkdirw(path)
char *path;
{
	char *p;
	int rc;
	char *strrchr();

	p = strrchr (path, '/');
	if (p != path) {
		*p = '\0';
		rc = access (path, W_OK);
		*p = '/';
	} else {
		rc = access ("/", W_OK);
	}
	if (rc < 0)
		return (-errno);
	else
		return (0);
}
