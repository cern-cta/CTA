/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: writelbl.c,v $ $Revision: 1.4 $ $Date: 2001/01/24 08:40:48 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	writelbl - write one label record */
/*	return	-1 and serrno set in case of error
 *		0	if ok
 */
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "serrno.h"
writelbl(tapefd, path, lblbuf)
int tapefd;
char *path;
char *lblbuf;
{
	char func[16];
	int n;

	ENTRY (writelbl);
	if ((n = write (tapefd, lblbuf, 80)) < 0) {
		serrno = rpttperror (func, tapefd, path, "write");
		RETURN (-1);
	}
	RETURN (n);
}
