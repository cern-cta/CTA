/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: cleaner.c,v $ $Revision: 1.5 $ $Date: 1999/12/08 15:57:24 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <sys/types.h>
#define RFIO_KERNEL 1
#include "rfio.h"
#include "stage.h"
char func[16];
int reqid;
static RFILE *rf;
main(argc, argv)
int argc;
char **argv;
{
	char buf[256];
	int c;
	char command[MAXPATH+MAXPOOLNAMELEN+2];
	char *gc;
	char *p;
	char *poolname;
	char *q;
	char savebuf[256];
	int saveflag = 0;
	char *hostname = "";

	strcpy (func, "cleaner");
	stglogit (func, "function entered\n");
	gc = argv[1];
	poolname = argv[2];
	if (argc == 4) hostname = argv[3];

	/* send garbage collector request to the disk server */

	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);
	sprintf (command, "%s %s %s", gc, poolname, hostname);
	rf = rfio_popen (command, "r");
	if (rf == NULL) {
		stglogit (func, "garbage collector %s failed to start on pool %s@%s\n",
			gc, poolname, hostname);
		exit (SYERR);
	}
	stglogit (func, "garbage collector %s started on pool %s@%s\n",
		gc, poolname, hostname);
	while ((c = rfio_pread (buf, 1, sizeof(buf)-1, rf)) > 0) {
		buf[c] = 0;
		p = buf;
		if (saveflag) {
			q = strchr (p, '\n');
			if (! q) {	/* line is still incomplete */
				strcat (savebuf, p);
				continue;
			}
			*q = '\0';
			strcat (savebuf, p);
			stglogit (func, "%s\n", savebuf);
			saveflag = 0;
			p = q + 1;
		}
		while (q = strchr (p, '\n')) {
			*q = '\0';
			stglogit (func, "%s\n", p);
			p = q + 1;
		}
		if (strlen (p)) {	/* save incomplete line */
			strcpy (savebuf, p);
			saveflag = 1;
		}
	}
	if (saveflag)
		stglogit (func, "%s\n", savebuf);
	c = rfio_pclose (rf);
	c = (c & 0xFF) ? SYERR : ((c >> 8) & 0xFF);
	exit (c);
}
