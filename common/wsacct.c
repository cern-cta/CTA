/*
 * $Id: wsacct.c,v 1.3 1999/12/09 13:39:50 jdurand Exp $
 */

/*
 * Copyright (C) 1994-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: wsacct.c,v $ $Revision: 1.3 $ $Date: 1999/12/09 13:39:50 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include "../h/sacct.h"
wsacct(package, acctstruct, len)
int package;
char *acctstruct;
int len;
{
	struct accthdr accthdr;
	char *buf;
	int fd_acct;

	accthdr.timestamp = time (0);
	accthdr.package = package;
	accthdr.len = len;

	buf = (char *) malloc (sizeof(struct accthdr) + len);
	memcpy (buf, (char *) &accthdr, sizeof(struct accthdr));
	memcpy (buf+sizeof(struct accthdr), acctstruct, len);
	fd_acct = open (ACCTFILE, O_WRONLY | O_CREAT | O_APPEND, 0666);
	(void) chmod (ACCTFILE, 0666);
	write (fd_acct, buf, sizeof(struct accthdr) + len);
        close (fd_acct);
	free (buf);
}
