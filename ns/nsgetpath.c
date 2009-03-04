/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsgetpath - get the path of a castorfile given by its fileid and name server where it resides */
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <errno.h>
#include <string.h>
#include "u64subr.h"
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "getconfent.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION] <CNSHOST> [FILEID|-x HEXID]\n", name);
    printf ("Resolve a fileid to a filepath.\n\n");
    printf ("  -x, --hex=HEXID       the fileid represented in hexidecimal\n");
    printf ("  -i, --ignore          ignore error messages related to forced CNS hosts\n");
    printf ("      --help            display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc, char**argv) {

  int c;
  int errflg = 0;
  int hflg = 0;
  int iflg = 0;
  int i = 0;
  int j = 0;
  u_signed64 fileid = 0;
  char *server = NULL;
  char filepath[CA_MAXPATHLEN + 1];
  char tmpbuf[21];
  char *p = NULL;

  Coptions_t longopts[] = {
    { "help",   NO_ARGUMENT,       &hflg, 1  },
    { "hex",    REQUIRED_ARGUMENT, NULL, 'x' },
    { "ignore", NO_ARGUMENT,       NULL, 'i' },
    { NULL,     0,                 NULL,  0  }
  };
  
  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "x:i", longopts, NULL)) != EOF) {
    switch (c) {
    case 'x':
      fileid = strtoull(Coptarg, NULL, 16);
      if (errno != 0) {
        fprintf(stderr, "invalid hexadecimal number: %s, %s\n",
                Coptarg, strerror(errno));
        exit (USERR);
      }
      break;
    case 'i':
      iflg++;
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }
  for (i = Coptind, j = 0; i < argc && j < 2; i++, j++) {
    if (j == 0) {
      server = argv[i];
    } else if (fileid == 0) {
      fileid = strtou64(argv[i]);
    }
  }
  if (hflg) {
    usage (0, argv[0]);
  }

  if (errflg || (server == NULL) || (fileid == 0)) {
    usage (USERR, argv[0]);
  }
  if (!iflg) {
    if ((p = getenv (CNS_HOST_ENV)) ||
	(p = getconfent (CNS_SCE, "HOST", 0))) {
      if (strcmp(p, server) != 0) {
	fprintf (stderr,
		 "cannot query '%s', all name server commands are forced to "
		 "query '%s'\n", server, p);
	exit (USERR);
      }
    }
  }
  if (Cns_getpath(server, fileid, filepath) != 0) {
    fprintf(stderr, "server %s fileid %s: %s\n",
            server, u64tostr (fileid, tmpbuf, 0), sstrerror(serrno));
    exit (USERR);
  }

  printf("%s\n", filepath);
  exit (0);
}
