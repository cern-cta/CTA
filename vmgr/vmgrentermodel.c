/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrentermodel.c,v $ $Revision: 1.4 $ $Date: 2000/03/05 16:13:34 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrentermodel - enter a new model of cartridge */
#include <stdio.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "u64subr.h"
#include "vmgr_api.h"
main(argc, argv)
int argc;
char **argv;
{
	int c;
	char *dp;
	int errflg = 0;
	static struct Coptions longopts[] = {
		{"media_cost", REQUIRED_ARGUMENT, 0, OPT_MEDIA_COST},
		{"mc", REQUIRED_ARGUMENT, 0, OPT_MEDIA_COST},
		{"media_letter", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"ml", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{"mo", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{"native_capacity", REQUIRED_ARGUMENT, 0, OPT_NATIVE_CAPACITY},
		{"nc", REQUIRED_ARGUMENT, 0, OPT_NATIVE_CAPACITY},
		{0, 0, 0, 0}
	};
	int media_cost = 0;
	char *media_letter = NULL;
	char *model = NULL;
	int native_capacity = 0;

	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_MEDIA_COST:
			if ((media_cost = strtol (Coptarg, &dp, 10)) <= 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid media_cost %s\n", Coptarg);
				errflg++;
			}
			break;
                case OPT_MEDIA_LETTER:
			media_letter = Coptarg;
                        break;
                case OPT_MODEL:
			model = Coptarg;
                        break;
		case OPT_NATIVE_CAPACITY:
			native_capacity = strutou64 (Coptarg) / ONE_MB;
			break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (Coptind < argc || model == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s%s", argv[0],
		    "--mo model [--ml media_letter] [--mc media_cost]\n",
		    "--nc native_capacity\n");
                exit (USERR);
        }
 
	if (vmgr_entermodel (model, media_letter, native_capacity, media_cost) < 0) {
		fprintf (stderr, "vmgrentermodel %s: %s\n", model, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
