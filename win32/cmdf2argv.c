/*
 * $Id: cmdf2argv.c,v 1.2 1999/12/09 13:47:50 jdurand Exp $
 */

/*
 * Copyright (C) 1997 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: cmdf2argv.c,v $ $Revision: 1.2 $ $Date: 1999/12/09 13:47:50 $ CERN/IT/PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	cmdf2argv - build argv array from a command file */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
extern char *sys_errlist[];

cmdf2argv(cmdfil, argvp)
char *cmdfil;
char ***argvp;
{
	char **argv;
	char *buf;
	int c;
	int cmdfd;
	int n;
	int nargs;
	char *p;
	int parm;
	struct stat st;

	if ((cmdfd = open (cmdfil, O_RDONLY)) < 0) {
		fprintf (stderr, "Command file open error: %s\n", sys_errlist[errno]);
		return (-1);
	}
	fstat (cmdfd, &st);
	if (st.st_size == 0) return (0);
	buf = (char *) malloc (st.st_size + 1);
	n = read (cmdfd, buf, st.st_size);
	close (cmdfd);
	*(buf+n) = '\0';
	
	/* 1st pass: count number of args */

	nargs = 0;
	p = buf;
	parm = 0;
	while (c = *p++) {
		if (c == ' ' || c == '\t' || c == '\n') {
			parm = 0;
		} else if (!parm) {
			parm = 1;
			nargs++;
		}
	}
	if (nargs == 0) return (0);
	argv = (char **) malloc ((nargs + 1) * sizeof(char *));

	/* 2nd pass: build argv */

	nargs = 0;
	p = buf;
	parm = 0;
	while (c = *p++) {
		if (c == ' ' || c == '\t' || c == '\n') {
			if (parm) {
				parm = 0;
				*(p - 1) = '\0';
			}
		} else if (!parm) {
			parm = 1;
			argv[nargs++] = p - 1;
		}
	}
	argv[nargs] = NULL;
	*argvp = argv;
	return (nargs);
}
