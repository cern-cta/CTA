/*
 * Copyright (C) 2000-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	vmgrentermodel - enter a new model of cartridge */
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include "Cgetopt_trunk_r21843.h"
#include "serrno_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"

int main(int argc,
         char **argv)
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
		{0, 0, 0, 0}
	};
	int media_cost = 0;
	char *media_letter = NULL;
	char *model = NULL;

	Copterr = 1;
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
                fprintf (stderr, "usage: %s %s", argv[0],
		    "--mo model [--ml media_letter] [--mc media_cost]\n");
                exit (USERR);
        }
 
	if (vmgr_entermodel (model, media_letter, media_cost) < 0) {
		fprintf (stderr, "vmgrentermodel %s: %s\n", model, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
