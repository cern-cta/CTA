/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: chkdirw.c,v $ $Revision: 1.5 $ $Date: 2007/02/21 16:31:31 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	chkdirw - extract directory name from full pathname
 *		and check if writable.
 *
 *	return	-1 with serrno set in case of error
 *		0	if writable
 */
#include <errno.h>
#include <string.h>
#if defined(_WIN32)
#define W_OK 2
#else
#include <unistd.h>
#endif
#include "serrno.h"
int chkdirw(path)
char *path;
{
	char *p;
	int rc;

	p = strrchr (path, '/');
	if (p != path) {
		*p = '\0';
		rc = access (path, W_OK);
		*p = '/';
	} else {
		rc = access ("/", W_OK);
	}
	if (rc < 0) {
		serrno = errno;
		return (-1);
	} else
		return (0);
}
