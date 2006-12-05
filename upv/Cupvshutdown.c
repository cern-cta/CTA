/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)Cupvshutdown.c,v 1.2 2002/06/12 08:17:11 CERN IT-DS/HSM Ben Couturier";
#endif /* not lint */

/*	Cupvshutdown - shutdown the UPV */
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
#include "Cupv.h"
#include "Cupv_util.h"

int main(int argc,char **argv)
{
#if defined(_WIN32)
      WSADATA wsadata;
#endif
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
		fprintf (stderr, CUP52);
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
 
#ifndef USE_CUPV
	fprintf(stderr,"Permission denied\n");
	exit(USERR);
#else
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CUPV_MAGIC);
	marshall_LONG (sbp, CUPV_SHUTDOWN);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_WORD (sbp, force);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */


#if defined(_WIN32)
    if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
      fprintf (stderr, CUP52);
      exit (SYERR);
	}
#endif

	if (send2Cupv (NULL, sendbuf, msglen, NULL, 0) < 0) {
		fprintf (stderr, "Cupvshutdown: %s\n", sstrerror(serrno));
#if defined(_WIN32)
           WSACleanup();
#endif
		 exit (USERR);
	}
#if defined(_WIN32)
      WSACleanup();
#endif
#endif
	  exit (0);
}


















