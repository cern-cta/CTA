/*
 * Copyright (C) 2000-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrenterdenmap.c,v $ $Revision: 1.3 $ $Date: 2003/10/29 07:48:58 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrenterdenmap - enter a new quadruplet model/media_letter/density/capacity */
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
	int errflg = 0;
	static struct Coptions longopts[] = {
		{"media_letter", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"ml", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{"mo", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{"native_capacity", REQUIRED_ARGUMENT, 0, OPT_NATIVE_CAPACITY},
		{"nc", REQUIRED_ARGUMENT, 0, OPT_NATIVE_CAPACITY},
		{0, 0, 0, 0}
	};
	char *density = NULL;
	char *media_letter = NULL;
	char *model = NULL;
	int native_capacity = 0;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "d:", longopts, NULL)) != EOF) {
                switch (c) {
                case 'd':
			density = Coptarg;
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
        if (Coptind < argc || model == NULL || density == NULL ||
	    native_capacity <= 0) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s%s", argv[0],
		    "-d density --mo model [--ml media_letter]\n",
		    "--nc native_capacity\n");
                exit (USERR);
        }
 
	if (vmgr_enterdenmap (model, media_letter, density, native_capacity) < 0) {
		fprintf (stderr, "vmgrenterdenmap %s: %s\n", model, sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
