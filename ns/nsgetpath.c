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
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s <CNSHOST> [FILEID|-x HEXID]\n", name);
    printf ("Resolve a fileid to a filepath.\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc, char**argv) {

  int c;
  int errflg = 0;
  int hflg = 0;
  u_signed64 fileid = -1;
  char *server = NULL;
  char filepath[CA_MAXPATHLEN + 1];
  char tmpbuf[21];

  Coptind = 1;
  if (argc >= 3) {
    server = argv[1];
    fileid = strtou64(argv[2]);
    Coptind = 2;
  }

  Coptions_t longopts[] = {
    { "help", NO_ARGUMENT, &hflg, 1  },
    { NULL,   0,           NULL,  0  }
  };

  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "x:", longopts, NULL)) != EOF) {
    switch (c) {
    case 'x':
      fileid = strtoull(Coptarg, NULL, 16);
      if (errno != 0) {
        fprintf(stderr, "invalid hexadecimal number: %s, %s\n",
                Coptarg, strerror(errno));
        exit (USERR);
      }
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (hflg) {
    usage (0, argv[0]);
  }
  if (errflg || (server == NULL) || (fileid == -1)) {
    usage (USERR, argv[0]);
  }

  if (Cns_getpath(server, fileid, filepath) != 0) {
    fprintf(stderr, "server %s fileid %s: %s\n",
            server, u64tostr (fileid, tmpbuf, 0), sstrerror(serrno));
    exit (USERR);
  }

  printf("%s\n", filepath);
  exit (0);
}
