/*
 * Copyright (C) 2003 by CERN/IT/GD/CT
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrdeltag.c,v $ $Revision: 1.1 $ $Date: 2003/10/28 11:13:25 $ CERN IT-GD/CT Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrdeltag - delete a tag associated with a tape volume */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "serrno.h"
#include "vmgr.h"
#include "vmgr_api.h"
extern	char	*optarg;
extern	int	optind;
main(argc, argv)
int argc;
char **argv;
{
	int c;
	int errflg = 0;
	char *vid = NULL;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	while ((c = getopt (argc, argv, "V:")) != EOF) {
		switch (c) {
		case 'V':
			vid = optarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (optind < argc || vid == NULL) {
		fprintf (stderr,
		    "usage: %s -V vid\n", argv[0]);
		exit (USERR);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, VMG52);
		exit (SYERR);
	}
#endif
	if (vmgr_deltag (vid)) {
		fprintf (stderr, "%s: %s\n", vid, sstrerror(serrno));
		errflg++;
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	if (errflg)
		exit (USERR);
	exit (0);
}
