/*
 * Copyright (C) 2000-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nssetcomment - add/replace a comment associated with a file/directory */
#include <errno.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s PATH COMMENT\n", name);
    printf ("Add/replace a comment associated with a file/directory\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(argc, argv)
     int argc;
     char **argv;
{
  int errflg = 0;
  int hflg = 0;
  int c;
  char fullpath[CA_MAXPATHLEN+1];
  char *p;
  char *path;

  Coptions_t longopts[] = {
    { "help",      NO_ARGUMENT, &hflg, 1  },
    { NULL,        0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
    switch (c) {
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
  if (argc < 3) {
    usage (USERR, argv[0]);
  }
  path = argv[1];
  if (*path != '/' && strstr (path, ":/") == NULL) {
    if ((p = getenv (CNS_HOME_ENV)) == NULL ||
        strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: invalid path\n", path);
      errflg++;
    } else
      sprintf (fullpath, "%s/%s", p, path);
  } else {
    if (strlen (path) > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: %s\n", path,
               sstrerror(SENAMETOOLONG));
      errflg++;
    } else
      strcpy (fullpath, path);
  }
  if (errflg == 0 && Cns_setcomment (fullpath, argv[2])) {
    fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
    errflg++;
  }
  if (errflg)
    exit (USERR);
  exit (0);
}
