/*
 * $Id: rfcat.c,v 1.6 2006/04/28 16:24:46 gtaur Exp $
 */

/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfcat.c,v $ $Revision: 1.6 $ $Date: 2006/04/28 16:24:46 $ CERN/IT/PDP/DM Jean-Philippe Baud";
#endif /* not lint */


#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "rfio_api.h"

main(argc, argv)
int argc;
char **argv;
{
	int errflg = 0;
	int i;
#if defined(_WIN32)
	WSADATA wsadata;

	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, "WSAStartup unsuccessful\n");
		exit (2);
	}
#endif

	if (argc == 1)
		errflg = catfile ("-");
	else
		for (i = 1; i < argc; i++)
			errflg += catfile (argv[i]);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (errflg ? 1 : 0);
}

catfile(inpfile)
char *inpfile;
{
	char buf [32768];
	int c;
	int rc;
	FILE *s;
	int v;

	/* Streaming opening is always better */
	v = RFIO_STREAM;
	rfiosetopt (RFIO_READOPT, &v, 4); 
	
	if (strcmp (inpfile, "-") == 0)
		s  = stdin;
	else if ((s = rfio_fopen64 (inpfile, "r")) == NULL) {
		rfio_perror (inpfile);
		return (1);
	}
	while ((c = rfio_fread (buf, 1, sizeof(buf), s)) > 0) {
		if ((rc = fwrite (buf, 1, c, stdout)) < c) {
			fprintf (stderr, "rfcat %s: %s\n", inpfile,
			    strerror ((rc < 0) ? errno : ENOSPC));
			if (strcmp (inpfile, "-"))
				rfio_fclose (s);
			return (1);
		}
	}
	if (strcmp (inpfile, "-"))
		rfio_fclose (s);
	return (0);
}
