/*
 * Copyright (C) 1994 by CERN/CN/PDP/DH
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)wsacct.c	1.2 08/24/94 CERN CN-PDP/DH Jean-Philippe Baud";
#endif /* not lint */

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
