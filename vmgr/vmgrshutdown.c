/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	vmgrshutdown - shutdown the volume manager */
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
extern	int	optind;
#include "vmgr.h"
#include "vmgr_api.h"

int main(argc, argv)
int argc;
char **argv;
{
	int c;
	int errflg = 0;
	int force = 0;
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;

	uid = geteuid();
	gid = getegid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, VMG53);
		exit (USERR);
	}
#endif
        while ((c = getopt (argc, argv, "f")) != EOF) {
                switch (c) {
                case 'f':
                        force++;
                        break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (optind < argc) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s [-f]\n", argv[0]);
                exit (USERR);
        }
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, VMGR_MAGIC);
	marshall_LONG (sbp, VMGR_SHUTDOWN);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_WORD (sbp, force);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	if (send2vmgr (NULL, sendbuf, msglen, NULL, 0) < 0) {
		fprintf (stderr, "vmgrshutdown: %s\n", sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
