/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrentertape.c,v $ $Revision: 1.5 $ $Date: 2000/03/05 16:30:03 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrentertape - enter a new tape volume */
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
		{"media_letter", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"ml", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{"pool", REQUIRED_ARGUMENT, 0, 'P'},
		{"sn", REQUIRED_ARGUMENT, 0, OPT_SN},
		{"status", REQUIRED_ARGUMENT, 0, OPT_STATUS},
		{0, 0, 0, 0}
	};
	char *manufacturer = NULL;
	char *media_letter = NULL;
	char *model = NULL;
	char *pool_name = NULL;
	char *sn = NULL;
	int status = 0;
	char *vid = NULL;
	char *vsn = NULL;

	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "d:g:l:P:V:v:", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_MANUFACTURER:
			manufacturer = Coptarg;
                        break;
                case OPT_MEDIA_LETTER:
			media_letter = Coptarg;
                        break;
                case OPT_MODEL:
			model = Coptarg;
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
        if (Coptind < argc || vid == NULL || dgn == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s%s%s", argv[0],
		    "[-d density] -g dgn [-l lbltype] [-P pool_name] -V vid\n",
		    "[-v vsn] [--ma manufacturer] [--ml media_letter] --mo model\n",
		    "[--pool pool_name] [--sn serial_number] [--st status]\n");
                exit (USERR);
        }
 
	if (vmgr_entertape (vid, vsn, dgn, density, lbltype, model,
	    media_letter, manufacturer, sn, pool_name, status) < 0) {
		fprintf (stderr, "vmgrentertape %s: %s\n", vid, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
