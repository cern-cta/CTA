/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrdeletedenmap.c,v $ $Revision: 1.3 $ $Date: 2005/07/11 11:43:27 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrdeletedenmap - delete a triplet model/media_letter/density */
#include <stdlib.h>
#include <errno.h>
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
	int errflg = 0;
	static struct Coptions longopts[] = {
		{"media_letter", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"ml", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{0, 0, 0, 0}
	};
	char *media_letter = NULL;
	char *model = NULL;

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
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (Coptind < argc || model == NULL || density == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s -d density --mo model [--ml media_letter]\n",
		    argv[0]);
                exit (USERR);
        }
 
	if (vmgr_deletedenmap (model, media_letter, density) < 0) {
		fprintf (stderr, "vmgrdeletedenmap %s, %s: %s\n", model, density,
		    (serrno == ENOENT) ? "No such denmap" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
