/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlfdeletetext.c,v $ $Revision: 1.1 $ $Date: 2003/08/20 13:01:30 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
#include "dlf.h"
#include "dlf_api.h"

extern	char	*optarg;
extern	int	optind;

main(argc, argv)
int argc;
char **argv;
{
	int msg_no;
	int errflg;
	char c;
	char fac_name[DLF_MAXFACNAMELEN + 1];
	char *endptr;
	int fac_name_set = 0;
	int msg_no_set = 0;

	errflg = 0;
        while ((c = getopt (argc, argv, "F:n:t:?")) != EOF) {
                switch (c) {
		case 'F':
			if (strlen (optarg) <= DLF_MAXFACNAMELEN) {
				strcpy (fac_name, optarg);
				fac_name_set++;
			}
			else {
				fprintf (stderr, "%s\n", strerror(EINVAL));
				errflg++;
			}
                        break;
		case 'n':
		        msg_no = strtol(optarg, &endptr, 10);
			if (*endptr != '\0' || msg_no < 0 || msg_no > 65535) {
				fprintf (stderr, "%s\n", strerror(EINVAL));
				errflg++;
			}
			else {
			  msg_no_set++;
			}
                        break;
                case '?':
                        errflg++;
                        break;
                default:
                        errflg++;
			break;
		}
	}
        if (optind < argc || !fac_name_set || !msg_no_set) {
                errflg++;
	}
        if (errflg)  {
                fprintf (stderr, "usage: %s %s", argv[0],
		    "-F facility_name -n text_number");
                exit (USERR);
	}

 
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, DLF52);
		exit (SYERR);
	}
#endif
	if (dlf_init("DLF-CONTROL") < 0 ) {
	    fprintf (stderr, "Error in initializing global structure.\n");
	    exit (SYERR);
	}
	if (dlf_deletetext(fac_name, msg_no) < 0)
	    exit (SYERR);

#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}
