/*
 * Copyright (C) 2005 by CERN/IT/GD/SC
 * All rights reserved
 */

/*      nsmodifyusrmap - modify user entry corresponding to a given uid */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "Cns_api.h"
#include "serrno.h"

main(argc, argv)
     int argc;
     char **argv;
{
  int c;
  char *dp;
  int errflg = 0;
  static struct Coptions longopts[] = {
    {"uid", REQUIRED_ARGUMENT, 0, OPT_IDMAP_UID},
    {"user", REQUIRED_ARGUMENT, 0, OPT_IDMAP_USER},
    {0, 0, 0, 0}
  };
  uid_t uid = 0;
  char *username = NULL;

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
    switch (c) {
    case OPT_IDMAP_UID:
      if ((uid = strtol (Coptarg, &dp, 10)) < 0 || *dp != '\0') {
        fprintf (stderr, "invalid uid: %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_IDMAP_USER:
      if (strlen (Coptarg) > 255) {
        fprintf (stderr,
                 "user name too long: %s\n", Coptarg);
        errflg++;
      } else
        username = Coptarg;
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (Coptind < argc || uid <= 0 || username == NULL)
    errflg++;
  if (errflg) {
    fprintf (stderr, "usage: %s %s", argv[0],
             "--uid uid --user username\n");
    exit (USERR);
  }

  if (Cns_modifyusrmap (uid, username) < 0) {
    if (serrno == EEXIST)
      fprintf (stderr, "nsmodifyusrmap %s: %s\n", username,
               "User exists already");
    else
      fprintf (stderr, "nsmodifyusrmap %d: %s\n", uid,
               (serrno == ENOENT) ? "No such user" : sstrerror(serrno));
    exit (USERR);
  }
  exit (0);
}
