/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrentertape.c,v $ $Revision: 1.8 $ $Date: 2002/02/07 06:07:53 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrentertape - enter a new tape volume */
#include <stdio.h>
#include <string.h>
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
	char *dp;
	int errflg = 0;
	char *lbltype = NULL;
	char *library = NULL;
	static struct Coptions longopts[] = {
		{"library", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
		{"manufacturer", REQUIRED_ARGUMENT, 0, OPT_MANUFACTURER},
		{"media_letter", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"ml", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{"nbsides", REQUIRED_ARGUMENT, 0, OPT_NBSIDES},
		{"pool", REQUIRED_ARGUMENT, 0, 'P'},
		{"sn", REQUIRED_ARGUMENT, 0, OPT_SN},
		{"status", REQUIRED_ARGUMENT, 0, OPT_STATUS},
		{0, 0, 0, 0}
	};
	char *manufacturer = NULL;
	char *media_letter = NULL;
	char *model = NULL;
	int nbsides = 1;
	char *p;
	char *pool_name = NULL;
	char *sn = NULL;
	int status = 0;
	char statusa[50];
	char *vid = NULL;
	char *vsn = NULL;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "d:l:P:V:v:", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_LIBRARY_NAME:
			library = Coptarg;
			break;
		case OPT_MANUFACTURER:
			manufacturer = Coptarg;
                        break;
                case OPT_MEDIA_LETTER:
			media_letter = Coptarg;
                        break;
                case OPT_MODEL:
			model = Coptarg;
                        break;
                case OPT_NBSIDES:
			if ((nbsides = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid number of sides %s\n", Coptarg);
				errflg++;
			}
                        break;
                case OPT_SN:
			sn = Coptarg;
                        break;
		case OPT_STATUS:
			if (isdigit (*Coptarg)) {
				if ((status = strtol (Coptarg, &dp, 10)) < 0 ||
				    *dp != '\0') {
					fprintf (stderr,
					    "invalid status %s\n", Coptarg);
					errflg++;
				}
			} else {
				if (strlen (Coptarg) >= sizeof (statusa)) {
					fprintf (stderr,
					    "invalid status %s\n", Coptarg);
					errflg++;
					break;
				}
				status = 0;
				strcpy (statusa, Coptarg);
				p = strtok (statusa, "|");
				while (p) {
					if (strcmp (p, "DISABLED") == 0)
						status |= DISABLED;
					else if (strcmp (p, "EXPORTED") == 0)
						status |= EXPORTED;
					else if (strcmp (p, "TAPE_FULL") == 0)
						status |= TAPE_FULL;
					else if (strcmp (p, "FULL") == 0)
						status |= TAPE_FULL;
					else if (strcmp (p, "TAPE_RDONLY") == 0)
						status |= TAPE_RDONLY;
					else if (strcmp (p, "RDONLY") == 0)
						status |= TAPE_RDONLY;
					else if (strcmp (p, "ARCHIVED") == 0)
						status |= ARCHIVED;
					else {
						fprintf (stderr,
						    "invalid status %s\n", Coptarg);
						errflg++;
						break;
					}
					p = strtok (NULL, "|");
				}
			}
			break;
		case 'd':
			density = Coptarg;
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
        if (Coptind < argc || vid == NULL || library == NULL ||
	    density == NULL || model == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s%s%s%s", argv[0],
		    "-d density [-l lbltype] [-P pool_name] -V vid [-v vsn]\n",
		    "--library library_name [--ma manufacturer] [--ml media_letter]\n",
		    "--mo model [--nbsides n] [--pool pool_name] [--sn serial_number]\n",
		    "[--st status]\n");
                exit (USERR);
        }
 
	if (vmgr_entertape (vid, vsn, library, density, lbltype, model,
	    media_letter, manufacturer, sn, nbsides, pool_name, status) < 0) {
		fprintf (stderr, "vmgrentertape %s: %s\n", vid, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
