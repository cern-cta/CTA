/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	nsgetpath - get the path of a castorfile given by its fileid and name server where it resides */

#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <errno.h>
#include <string.h>
#include "u64subr.h"
#include "Cns_api.h"
#include "serrno.h"

int main(int argc, char**argv) {
  
	int c;
	int errflg = 0;
	u_signed64 fileid = 0;
	char *server = NULL;
	char filepath[CA_MAXPATHLEN + 1];
	char tmpbuf[21];

	if (argc >= 3) {
		server = argv[1];
		fileid = strtou64(argv[2]);
	}

	while ((c = getopt (argc, argv, "x:")) != EOF) {
		switch (c) {
		case 'x':
			fileid = strtoull(optarg, NULL, 16);
			if (errno != 0) {
				fprintf(stderr, "invalid hexadecimal number: %s, %s\n", 
					optarg, strerror(errno));
				exit(USERR);
			}
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	
	if (errflg || (server == NULL) || (fileid == 0)) {
		fprintf(stderr, "usage: %s server [fileid| -x hex]\n", argv[0]);
		exit(USERR);
	}
	
	if (Cns_getpath(server, fileid, filepath) != 0) {
		fprintf(stderr, "server %s fileid %s: %s\n",
			server, u64tostr (fileid, tmpbuf, 0), sstrerror(serrno));
		exit(USERR);
	}
	
	fprintf(stdout, "%s\n", filepath);
	exit(0);
}
