/*
 * $Id: stageping.c,v 1.7 2002/10/20 08:41:27 jdurand Exp $
 */

/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageping.c,v $ $Revision: 1.7 $ $Date: 2002/10/20 08:41:27 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include "Cpwd.h"
#include "Cgrp.h"
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "stage_api.h"
#include "Cgetopt.h"
#include "serrno.h"

void usage _PROTO((char *));
void cleanup _PROTO((int));

static int noretry_flag = 0;

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c;
	int errflg = 0;
	char *stghost = NULL;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	u_signed64 flags = 0;
	static struct Coptions longopts[] =
	{
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"verbose",            NO_ARGUMENT,        NULL,      'v'},
		{"noretry",            NO_ARGUMENT,     &noretry_flag,  1},
		{NULL,                 0,                  NULL,        0}
	};

	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt_long (argc, argv, "h:v", longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			stghost = Coptarg;
			break;
		case 'v':
			flags |= STAGE_VERBOSE;
			break;
		case 0:
			/* Here are the long options */
			break;
		case '?':
			errflg++;
			break;
		default:
			errflg++;
			break;
		}
        if (errflg != 0) break;
	}
	if (argc > Coptind) {
		fprintf (stderr, STG16);
		errflg++;
	}

	if (errflg != 0) {
		usage (argv[0]);
		exit (USERR);
	}

	if (noretry_flag) flags |= STAGE_NORETRY;

#if !defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if !defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);
	
	c = stage_ping(flags,stghost);

	exit (c == 0 ? 0 : rc_castor2shift(serrno));
}

void cleanup(sig)
		 int sig;
{
	signal (sig, SIG_IGN);
	
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (USERR);
}

void usage(cmd)
		 char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s", "[-h stage_host] [-v] [--noretry]\n");
}
