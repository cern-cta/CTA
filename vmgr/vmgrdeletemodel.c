/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrdeletemodel.c,v $ $Revision: 1.2 $ $Date: 2002/08/26 14:50:47 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrdeletemodel - delete a media model definition */
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
        while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
                switch (c) {
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
                fprintf (stderr, "usage: %s --mo model [--ml media_letter]\n",
		    argv[0]);
                exit (USERR);
        }
 
	if (vmgr_deletemodel (model, media_letter) < 0) {
		fprintf (stderr, "vmgrdeletemodel %s: %s\n", model,
		    (serrno == ENOENT) ? "No such model" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
