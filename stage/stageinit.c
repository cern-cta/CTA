/*
 * $Id: stageinit.c,v 1.22 2002/05/31 08:15:21 jdurand Exp $
 */

/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageinit.c,v $ $Revision: 1.22 $ $Date: 2002/05/31 08:15:21 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
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
	while ((c = Cgetopt (argc, argv, "Fh:X")) != -1) {
		switch (c) {
		case 'F':
			flags |= STAGE_FORCE;
			break;
		case 'h':
			stghost = Coptarg;
			break;
		case 'X':
			flags |= STAGE_MIGRINIT;
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
	if (errflg) {
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

	c = stage_init(flags,stghost);

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
	fprintf (stderr, "usage: %s [-F] [-h stage_host] [-X]\n", cmd);
}
