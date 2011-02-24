/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	vmgrmodifylibrary - modify an existing tape library definition */
#include <ctype.h>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "Cgetopt_trunk_r21843.h"
#include "serrno_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"


/**
 * Converts the specified string to uppercase.
 */
void toupperstr(char *str) {
  for(;*str != '\0'; str++) {
    *str = toupper(*str);
  }
}


int main(int argc,
         char **argv)
{
	int c;
	int capacity = -1;
	char *dp;
	int errflg = 0;
	char *library_name = NULL;
	static struct Coptions longopts[] = {
		{"capacity", REQUIRED_ARGUMENT, 0, OPT_CAPACITY},
		{"name", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
		{"status", REQUIRED_ARGUMENT, 0, OPT_STATUS},
		{0, 0, 0, 0}
	};
	int status = -1;

	Copterr = 1;
	Coptind = 1;
        while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
                switch (c) {
		case OPT_CAPACITY:
			if ((capacity = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid capacity %s\n", Coptarg);
				errflg++;
			}
			break;
		case OPT_LIBRARY_NAME:
			library_name = Coptarg;
			break;
		case OPT_STATUS:
			toupperstr(Coptarg);

			if(strcmp(Coptarg, "ONLINE") == 0) {
				status = LIBRARY_ONLINE;
			} else if(strcmp(Coptarg, "OFFLINE") == 0) {
				status = LIBRARY_OFFLINE;
			} else {
				fprintf (stderr,
				    "invalid status %s\n", Coptarg);
				errflg++;
			}
			break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (Coptind < argc) {
                errflg++;
        }
	if (library_name == NULL) {
		fprintf (stderr, "no library_name\n");
                errflg++;
	}
        if (errflg) {
		fprintf (stderr, "usage: %s %s", argv[0],
			 "--name library_name [--capacity n] [--status ONLINE|OFFLINE]\n");
                exit (USERR);
        }
 
	if (vmgr_modifylibrary (library_name, capacity, status) < 0) {
		fprintf (stderr, "vmgrmodifylibrary %s: %s\n", library_name,
			 sstrerror(serrno));
		exit (USERR);
	}
	exit (0);
}
