/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrmodifytape.c,v $ $Revision: 1.4 $ $Date: 2001/02/21 06:05:23 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrmodifytape - modify an existing tape volume */
#include <stdio.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "vmgr_api.h"
main(argc, argv)
int argc;
char **argv;
{
	int c;
	char *density = NULL;
	char *dgn = NULL;
	char *dp;
	int errflg = 0;
	char *lbltype = NULL;
	static struct Coptions longopts[] = {
		{"manufacturer", REQUIRED_ARGUMENT, 0, OPT_MANUFACTURER},
		{"pool", REQUIRED_ARGUMENT, 0, 'P'},
		{"sn", REQUIRED_ARGUMENT, 0, OPT_SN},
		{"status", REQUIRED_ARGUMENT, 0, OPT_STATUS},
		{0, 0, 0, 0}
	};
	char *manufacturer = NULL;
	char *pool_name = NULL;
	char *sn = NULL;
	int status = -1;
	char *vid = NULL;
	char *vsn = NULL;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "d:g:l:P:V:v:", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_MANUFACTURER:
			manufacturer = Coptarg;
                        break;
                case OPT_SN:
			sn = Coptarg;
                        break;
		case OPT_STATUS:
			if ((status = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid status %s\n", Coptarg);
				errflg++;
			}
			break;
		case 'd':
			density = Coptarg;
			break;
		case 'g':
			dgn = Coptarg;
			break;
		case 'l':
			lbltype = Coptarg;
			break;
		case 'P':
			pool_name = Coptarg;
			break;
		case 'V':
			vid = Coptarg;
			break;
		case 'v':
			vsn = Coptarg;
			break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (Coptind < argc) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s%s%s", argv[0],
		    "[-d density] [-g dgn] [-l lbltype] [-P pool_name] -V vid\n",
		    "[-v vsn] [--ma manufacturer] [--pool pool_name]\n",
		    "[--sn serial_number] [--st status]\n");
                exit (USERR);
        }
 
	if (vmgr_modifytape (vid, vsn, dgn, density, lbltype,
	    manufacturer, sn, pool_name, status) < 0) {
		fprintf (stderr, "vmgrmodifytape %s: %s\n", vid, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
