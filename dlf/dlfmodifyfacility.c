/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlfmodifyfacility.c,v $ $Revision: 1.3 $ $Date: 2004/10/20 11:22:56 $ CERN IT-ADC/CA Vitaly Motyakov";
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
        int fac_no = -1;
	int errflg;
	char c;
	char *endptr;
	char fac_name[DLF_MAXFACNAMELEN + 1];
	int fac_name_set = 0;
	int fac_no_set = 0;
#if defined (_WIN32)
	WSADATA wsadata;
#endif

	errflg = 0;
        while ((c = getopt (argc, argv, "F:n:?")) != EOF) {
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
		        fac_no = strtol(optarg, &endptr, 10);
			if (*endptr != '\0' || fac_no < 0 || fac_no > 255) {
				fprintf (stderr, "%s\n", strerror(EINVAL));
				errflg++;
			}
			else {
			  fac_no_set++;
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
        if (optind < argc || !fac_name_set || !fac_no_set ) {
                errflg++;
	}
        if (errflg) {
                fprintf (stderr, "usage: %s %s\n", argv[0],
		    "-F facility_new_name -n facility_number");
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

	if (dlf_modifyfacility(fac_name, fac_no) < 0)
	    exit (SYERR);

#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}
