/*
 * Copyright (C) 2000-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	vmgrdeletetape - delete a tape volume */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include "serrno.h"
#include "vmgr_api.h"
extern	char	*optarg;
extern	int	optind;

int main(argc, argv)
int argc;
char **argv;
{
	int c;
	int errflg = 0;
	char *vid = NULL;

        while ((c = getopt (argc, argv, "V:")) != EOF) {
                switch (c) {
                case 'V':
			vid = optarg;
                        break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (optind < argc || vid == NULL) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s -V vid\n", argv[0]);
                exit (USERR);
        }
 
	if (vmgr_deletetape (vid) < 0) {
		fprintf (stderr, "vmgrdeletetape %s: %s\n", vid,
		    (serrno == ENOENT) ? "No such tape" : sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
