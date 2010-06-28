/*
 * Copyright (C) 2003-2004 by CERN/IT/GD/CT
 * All rights reserved
 */

/* nsln - make a symbolic link to a file or a directory */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s -s TARGET [LINK_NAME]\n", name);
    printf ("   or: %s -s TARGET... DIRECTORY\n", name);
    printf ("Create a symbolic link to a file or directory in the CASTOR name server.\n\n");
    printf ("  -s, --symbolic  make symbolic link\n");
    printf ("      --help      display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(argc, argv)
     int argc;
     char **argv;
{
  int c;
  int errflg = 0;
  int sflag = 0;
  int hflg = 0;
  char fullpath[CA_MAXPATHLEN+1];
  int i;
  char *lastarg;
  char *p;
  char path[CA_MAXPATHLEN+1];
  struct Cns_filestat statbuf;

  Coptions_t longopts[] = {
    { "symbolic", NO_ARGUMENT, NULL, 's' },
    { "help",     NO_ARGUMENT, &hflg, 1  },
    { NULL,       0,           NULL,  0  }
  };

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "s", longopts, NULL)) != EOF) {
    switch (c) {
    case 's':
      sflag++;
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
  if (! sflag)
    errflg++;
  if (errflg || Coptind >= argc) {
    usage (USERR, argv[0]);
  }
  if (argc - Coptind > 2) {
    if (Cns_stat (argv[argc-1], &statbuf) < 0 ||
        (statbuf.filemode & S_IFDIR) == 0) {
      fprintf (stderr, "target %s must be a directory\n",
               argv[argc-1]);
      exit (USERR);
    }
  }
  argc -= Coptind;
  argv += Coptind;
  if (argc == 1)
    if ((p = strrchr (argv[0], '/')))
      lastarg = p + 1;
    else
      lastarg = argv[0];
  else
    lastarg = argv[--argc];
  for (i = 0; i < argc; i++) {
    if (argc > 1)
      if ((p = strrchr (argv[i], '/')))
        sprintf (path, "%s/%s", lastarg, p + 1);
      else
        sprintf (path, "%s/%s", lastarg, argv[i]);
    else
      strcpy (path, lastarg);
    if (*path != '/' && strstr (path, ":/") == NULL) {
      if ((p = getenv (CNS_HOME_ENV)) == NULL ||
          strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
        fprintf (stderr, "%s: invalid path\n", path);
        errflg++;
        continue;
      } else
        sprintf (fullpath, "%s/%s", p, path);
    } else {
      if (strlen (path) > CA_MAXPATHLEN) {
        fprintf (stderr, "%s: %s\n", path,
                 sstrerror(SENAMETOOLONG));
        errflg++;
        continue;
      } else
        strcpy (fullpath, path);
    }
    if (Cns_symlink (argv[i], fullpath)) {
      fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}
