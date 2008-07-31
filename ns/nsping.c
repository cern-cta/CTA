/*
 * Copyright (C) 2007-2008 by CERN/IT/GD/ITR
 * All rights reserved
 */

/* nsping - check name server alive and return version number */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <getopt.h>
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

extern char *optarg;
extern int optind;

int main(argc, argv)
     int argc;
     char **argv;
{
  int c;
  int errflg = 0;
  char info[256];
  static char retryenv[16];
  char *server = NULL;

  while ((c = getopt (argc, argv, "h:")) != EOF) {
    switch (c) {
    case 'h':
      server = optarg;
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (optind < argc) {
    errflg++;
  }
  if (errflg) {
    fprintf (stderr, "usage: %s [-h name_server]\n", argv[0]);
    exit (USERR);
  }

  sprintf (retryenv, "%s=0", CNS_CONRETRY_ENV);
  putenv (retryenv);
  if (Cns_ping (server, info) < 0) {
    fprintf (stderr, "nsping: %s\n", sstrerror(serrno));
    exit (USERR);
  }
  printf ("%s\n", info);
  exit (0);
}
