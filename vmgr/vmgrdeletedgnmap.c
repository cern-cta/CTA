/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrdeletedgnmap.c,v $ $Revision: 1.1 $ $Date: 2002/01/18 09:02:20 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrdeletedgnmap - delete a triplet dgn/model/library */
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
	char *library = NULL;
	static struct Coptions longopts[] = {
		{"library", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{0, 0, 0, 0}
	};
	char *model = NULL;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
                switch (c) {
                case OPT_LIBRARY_NAME:
			library = Coptarg;
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
        if (Coptind < argc || library == NULL || model == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s --li library_name --mo model\n",
		    argv[0]);
                exit (USERR);
        }
 
	if (vmgr_deletedgnmap (model, library) < 0) {
		fprintf (stderr, "vmgrdeletedgnmap %s, %s: %s\n", model, library,
		    (serrno == ENOENT) ? "No such dgnmap" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
