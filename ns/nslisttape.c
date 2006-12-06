/*
 * Copyright (C) 2000-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nslisttape.c,v $ $Revision: 1.3 $ $Date: 2006/12/06 16:05:08 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*      nslisttape - list the file segments residing on a volume */
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <stdlib.h> 
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cgetopt.h"
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
#include "u64subr.h"
int main(int argc,char **argv)
{
	int c;
	static int dsflag = 0;
        static int checksumflag = 0;
	struct Cns_direntape *dtp;
	int errflg = 0;
	int flags;
	Cns_list list;
	static struct Coptions longopts[] = {
		{"display_side", NO_ARGUMENT, &dsflag, 1},
        {"checksum", NO_ARGUMENT, &checksumflag, 1},
		{"ds", NO_ARGUMENT, &dsflag, 1},
		{0, 0, 0, 0}
	};
	signed64 parent_fileid = -1;
	char path[CA_MAXPATHLEN+1];
	char *server = NULL;
	char tmpbuf[21];
	char *vid = NULL;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "h:V:", longopts, NULL)) != EOF) {
		switch (c) {
		case 'h':
			server = Coptarg;
			break;
		case 'V':
			vid = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (Coptind < argc || ! vid) {
		errflg++;
	}
	if (errflg) {
		fprintf (stderr, "usage: %s [-h server] [--display_side] [--checksum] -V vid\n", argv[0]);
		exit (USERR);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif

	flags = CNS_LIST_BEGIN;
	while ((dtp = Cns_listtape (server, vid, flags, &list)) != NULL) {
		if (dtp->parent_fileid != parent_fileid) {
			if (Cns_getpath (server, dtp->parent_fileid, path) < 0) {
				fprintf (stderr, "%s\n", sstrerror(serrno));
#if defined(_WIN32)
				WSACleanup();
#endif
				exit (USERR);
			}
			parent_fileid = dtp->parent_fileid;
		}
		if (dsflag || dtp->side > 0)
			printf ("%c %d %3d %-6.6s/%d %5d %02x%02x%02x%02x %s %3d",
			    dtp->s_status, dtp->copyno, dtp->fsec, dtp->vid,
			    dtp->side, dtp->fseq, dtp->blockid[0], dtp->blockid[1],
			    dtp->blockid[2], dtp->blockid[3],
			    u64tostr (dtp->segsize, tmpbuf, 20), dtp->compression);
		else
			printf ("%c %d %3d %-6.6s   %5d %02x%02x%02x%02x %s %3d",
			    dtp->s_status, dtp->copyno, dtp->fsec, dtp->vid,
			    dtp->fseq, dtp->blockid[0], dtp->blockid[1],
			    dtp->blockid[2], dtp->blockid[3],
			    u64tostr (dtp->segsize, tmpbuf, 20), dtp->compression);

        if (checksumflag) {
            if (dtp->checksum_name != NULL &&  strlen(dtp->checksum_name)>0) {
                printf (" %*s %08lx", CA_MAXCKSUMNAMELEN, dtp->checksum_name, dtp->checksum);
            } else {
		    printf (" %*s %08x", CA_MAXCKSUMNAMELEN, "-", 0);
            }
        }

        printf (" %s/%s\n",path, dtp->d_name);
		flags = CNS_LIST_CONTINUE;
	}
	(void) Cns_listtape (server, vid, CNS_LIST_END, &list);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}
