/*
 * $Id: stageshutdown.c,v 1.12 2003/09/08 15:52:30 jdurand Exp $
 */

/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageshutdown.c,v $ $Revision: 1.12 $ $Date: 2003/09/08 15:52:30 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/types.h>
#include <pwd.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <errno.h>
#include "marshall.h"
#include "stage_api.h"
#include "Cpwd.h"
#include "Cgetopt.h"
#include "serrno.h"

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
void cleanup _PROTO((int));
void usage _PROTO((char *));

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c;
	void cleanup();
	int errflg = 0;
	char *stghost = NULL;
	u_signed64 flags = 0;
	
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt (argc, argv, "Fh:")) != -1) {
		switch (c) {
		case 'F':
			flags |= STAGE_FORCE;
			break;
		case 'h':
			stghost = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			errflg++;
			break;
		}
	}
	if (argc > Coptind) {
		fprintf (stderr, STG16);
		errflg++;
	}
	/* We force -h parameter to appear in the parameters */
	if (stghost == NULL) {
		errflg++;
	}

	if (errflg != 0) {
		usage (argv[0]);
		exit (USERR);
	}

#if !defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if !defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);

	c = stage_shutdown(flags,stghost);

	exit (c == 0 ? 0 : rc_castor2shift(serrno));
}

void cleanup(sig)
		 int sig;
{
	signal (sig, SIG_IGN);

	exit (USERR);
}

void usage(cmd)
		 char *cmd;
{
	fprintf (stderr, "usage: %s -h stagehost [-F]\n", cmd);
}
