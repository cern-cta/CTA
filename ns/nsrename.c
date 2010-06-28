/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsrename - rename entries in name server */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... SOURCE DEST\n", name);
    printf ("Rename a file or directory in the name server.\n\n");
    printf ("  -f, --force    do not prompt before overwriting\n");
    printf ("  -v, --verbose  explain what is being done\n");
    printf ("      --help     display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int isyes()
{
  int c;
  int fchar;

  fchar = c = getchar();
  while (c != '\n' && c != EOF)
    c = getchar();
  return (fchar == 'y');
}

int main(argc, argv)
     int argc;
     char **argv;
{
  int c;
  char newpath[CA_MAXPATHLEN+1];
  char oldpath[CA_MAXPATHLEN+1];
  int errflg = 0;
  int fflag = 0;
  int hflg = 0;
  int vflg = 0;
  char *p;
  char *path;
  struct Cns_filestat statbuf;

  Coptions_t longopts[] = {
    { "force",   NO_ARGUMENT, NULL, 'f' },
    { "verbose", NO_ARGUMENT, NULL, 'v' },
    { "help",    NO_ARGUMENT, &hflg, 1  },
    { NULL,      0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "fv", longopts, NULL)) != EOF) {
    switch (c) {
    case 'f':
      fflag++;
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
  if (errflg || Coptind > (argc - 2)) {
    usage (USERR, argv[0]);
  }

  path = argv[Coptind];
  if (*path != '/' && strstr (path, ":/") == NULL) {
    if ((p = getenv (CNS_HOME_ENV)) == NULL ||
        strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: invalid path\n", path);
      errflg++;
    } else
      sprintf (oldpath, "%s/%s", p, path);
  } else {
    if (strlen (path) > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: %s\n", path,
               sstrerror(SENAMETOOLONG));
      errflg++;
    } else
      strcpy (oldpath, path);
  }
  path = argv[Coptind + 1];
  if (*path != '/' && strstr (path, ":/") == NULL) {
    if ((p = getenv (CNS_HOME_ENV)) == NULL ||
        strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: invalid path\n", path);
      errflg++;
    } else
      sprintf (newpath, "%s/%s", p, path);
  } else {
    if (strlen (path) > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: %s\n", path,
               sstrerror(SENAMETOOLONG));
      errflg++;
    } else
      strcpy (newpath, path);
  }
  if (errflg)
    exit (USERR);
  if (vflg)
    printf ("%s: renaming %s to %s\n", argv[0], oldpath, newpath);
  if (! fflag && Cns_lstat(newpath, &statbuf) == 0) {
    if (isatty(fileno(stdin))) {
      printf("%s: overwrite %s? ", argv[0], newpath);
      if (! isyes()) {
        exit (0);
      }
    }
  }

  if (Cns_rename (oldpath, newpath) < 0) {
    fprintf (stderr, "cannot rename to %s: %s\n", path,
             sstrerror(serrno));
    exit (USERR);
  }
  exit (0);
}
