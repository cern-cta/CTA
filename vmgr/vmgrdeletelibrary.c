/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrdeletelibrary.c,v $ $Revision: 1.2 $ $Date: 2002/01/18 08:15:11 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrdeletelibrary - delete a tape library definition */
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
		{"name", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
		{0, 0, 0, 0}
	};
	char *library_name = NULL;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
                switch (c) {
                case OPT_LIBRARY_NAME:
			library_name = Coptarg;
                        break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (Coptind < argc || library_name == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s --name library_name\n", argv[0]);
                exit (USERR);
        }
 
	if (vmgr_deletelibrary (library_name) < 0) {
		fprintf (stderr, "vmgrdeletelibrary %s: %s\n", library_name,
		    (serrno == ENOENT) ? "No such library" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
