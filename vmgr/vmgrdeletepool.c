/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrdeletepool.c,v $ $Revision: 1.2 $ $Date: 2002/01/18 08:15:10 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrdeletepool - delete a tape pool definition */
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
		{"name", REQUIRED_ARGUMENT, 0, 'P'},
		{0, 0, 0, 0}
	};
	char *pool_name = NULL;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
                switch (c) {
                case 'P':
			pool_name = Coptarg;
                        break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (Coptind < argc || pool_name == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s --name pool_name\n", argv[0]);
                exit (USERR);
        }
 
	if (vmgr_deletepool (pool_name) < 0) {
		fprintf (stderr, "vmgrdeletepool %s: %s\n", pool_name,
		    (serrno == ENOENT) ? "No such pool" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
