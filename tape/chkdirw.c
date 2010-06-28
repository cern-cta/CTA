/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	chkdirw - extract directory name from full pathname
 *		and check if writable.
 *
 *	return	-1 with serrno set in case of error
 *		0	if writable
 */
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include "serrno.h"
#include "Ctape_api.h"

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
