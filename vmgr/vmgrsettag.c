/*
 * Copyright (C) 2003 by CERN/IT/GD/CT
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrsettag.c,v $ $Revision: 1.1 $ $Date: 2003/10/28 11:13:25 $ CERN IT-GD/CT Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrsettag - add/replace a tag associated with a tape volume */
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cgetopt.h"
#include "serrno.h"
#include "vmgr.h"
#include "vmgr_api.h"
main(argc, argv)
int argc;
char **argv;
{
	int c;
	int errflg = 0;
	static struct Coptions longopts[] = {
		{"tag", REQUIRED_ARGUMENT, 0, OPT_TAG},
		{0, 0, 0, 0}
	};
	char *tag = NULL;
	char *vid = NULL;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "V:", longopts, NULL)) != EOF) {
		switch (c) {
		case 'V':
			vid = Coptarg;
			break;
		case OPT_TAG:
			tag = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (Coptind < argc || vid == NULL || tag == NULL) {
		fprintf (stderr,
		    "usage: %s --tag text -V vid\n", argv[0]);
		exit (USERR);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, VMG52);
		exit (SYERR);
	}
#endif
	if (vmgr_settag (vid, tag)) {
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
