/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlflisttext.c,v $ $Revision: 1.3 $ $Date: 2004/10/20 11:22:56 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "serrno.h"
#include "dlf.h"
#include "dlf_api.h"

extern	char	*optarg;
extern	int	optind;

main(argc, argv)
int argc;
char **argv;
{
	unsigned int fac_no;
	dlf_msg_text_slist_t text_list;
	dlf_msg_text_t* p;
	int errflg;
	char c;
	char fac_name[DLF_MAXFACNAMELEN + 1];
	int fac_name_set = 0;

#if defined(_WIN32)
	WSADATA wsadata;
#endif
	errflg = 0;
        while ((c = getopt (argc, argv, "F:?")) != EOF) {
	        switch (c) {
		case 'F':
			if (strlen (optarg) <= DLF_MAXFACNAMELEN) {
				strcpy (fac_name, optarg);
				fac_name_set++;
			}
			else {
				fprintf (stderr, "%s\n", sstrerror(EINVAL));
				errflg++;
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
        if (optind < argc || !fac_name_set) {
                errflg++;
	}
        if (errflg) {
                fprintf (stderr, "usage: %s %s\n", argv[0],
		    "-F facility_name");
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

	text_list.head = NULL;
	text_list.tail = NULL;

	if (dlf_gettexts(fac_name, &fac_no, &text_list) < 0) {
	    fprintf (stderr, "Error in getting texts from the server.\n");
	    exit (SYERR);
	}

	/* Print result */
	printf("\nFacility: %s, Facility number: %d\n\n", fac_name, fac_no);
	for (p = text_list.head; p != NULL; p = p->next) {
	    printf("%d: %s\n", p->msg_no, p->msg_text);
	}

#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}
