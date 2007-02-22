/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*	nsshutdown - shutdown the name server */
#include <stdio.h>
#include <sys/types.h>
#include <getopt.h>
#include <stdlib.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "Cns_api.h"
#include "Cns.h"
#include "marshall.h"
#include "serrno.h"
extern	char	*optarg;
extern	int	optind;

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
	char *server = NULL;
	uid_t uid;

	Cns_getid(&uid, &gid);
	
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, NS053);
		exit (USERR);
	}
#endif
        while ((c = getopt (argc, argv, "fh:")) != EOF) {
                switch (c) {
                case 'f':
                        force++;
                        break;
		case 'h':
			server = optarg;
			break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (optind < argc || server == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s [-f] -h server\n", argv[0]);
                exit (USERR);
        }
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_SHUTDOWN);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_WORD (sbp, force);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */

	if (send2nsd (NULL, server, sendbuf, msglen, NULL, 0) < 0) {
		fprintf (stderr, "nsshutdown: %s\n", sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
