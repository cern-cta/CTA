/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlfshutdown.c,v $ $Revision: 1.3 $ $Date: 2005/07/11 11:31:24 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */


/*	dlfshutdown - shutdown the dlf server */
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
extern	int	optind;
#include "dlf.h"
#include "dlf_api.h"

main(argc, argv)
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
	char sendbuf[DLF_REQBUFSZ];
	uid_t uid;
	int socket;
	dlf_log_dst_t* dst;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	uid = geteuid();
	gid = getegid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, DLF53);
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
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, DLF52);
		exit (SYERR);
	}
#endif
	if (dlf_init("DLF-CONTROL") < 0 )
	  {
	    fprintf (stderr, "Error in initializing global structure.\n");
	    exit (SYERR);
	  }

	if ((dst = dlf_find_server(DLF_LVL_SERVICE_ONLY)) == NULL)
	  exit (SYERR);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, DLF_MAGIC);
	marshall_LONG (sbp, DLF_SHUTDOWN);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_WORD (sbp, force);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	socket = -1;

	if (send2dlf (&socket, dst, sendbuf, msglen) < 0)
	  {
		fprintf (stderr, "dlfshutdown: %s\n", sstrerror(serrno));
		exit (USERR);
	  }

	exit (0);
}
