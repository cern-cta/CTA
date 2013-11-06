/*
 * Copyright (C) 1999-2004 by CERN/IT/DM
 * All rights reserved
 */

/* nsrmvid - remove all file entries on a given volume */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "u64subr.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... VID...\n", name);
    printf ("Remove all files on a given volume.\n\n");
    printf ("  -v, --verbose        output a line for every volume processed\n");
    printf ("  -h, --host=HOSTNAME  the name server to query\n");
    printf ("      --help           display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,
         char **argv)
{
  char *vid    = NULL;
  char *server = NULL;
  int  vflg    = 0;
  int  hflg    = 0;
  int  errflg  = 0;
  int  c;
  int  i;
  int  count;

  Coptions_t longopts[] = {
    { "verbose", NO_ARGUMENT,       NULL,  'v' },
    { "host",    REQUIRED_ARGUMENT, NULL,  'h' },
    { "help",    NO_ARGUMENT,       &hflg,  1  },
    { NULL,      0,                 NULL,   0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "vh:", longopts, NULL)) != EOF) {
    switch (c) {
    case 'h':
      server = Coptarg;
      setenv(CNS_HOST_ENV, server, 1);
      break;
    case 'v':
      vflg++;
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
  if (errflg || Coptind >= argc) {
    usage (USERR, argv[0]);
  }

  for (i = Coptind; i < argc; i++) {
    vid = argv[i];
    if (vflg) {
      printf ("%s: removing files on volume, %s\n", argv[0], vid);
    }
    count = 0;
    while ((c = Cns_unlinkbyvid(server, vid)) == 0) {
      count++;
    }
    if (c < 0) {
      if (count && (serrno == ENOENT)) {
        continue;
      }
      fprintf (stderr, "%s: %s: %s\n", argv[0], vid, (serrno == ENOENT) ?
               "No such volume or no files found" : sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}
