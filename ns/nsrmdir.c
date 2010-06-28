/*
 * Copyright (C) 1999-2004 by CERN/IT/DM
 * All rights reserved
 */

/* nsrmdir - remove empty directories */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
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
    printf ("Usage: %s [OPTION]... DIRECTORY...\n", name);
    printf ("Remove the DIRECTORY(ies), if they are empty.\n\n");
    printf ("  -i, --ignore-fail-on-non-empty\n");
    printf ("                 ignore each failure that is solely because a directory\n");
    printf ("                 is non-empty\n");
    printf ("  -p, --parents  remove directory and its ancestors\n");
    printf ("                 E.g. `nsrmdir -p a/b/c` is similar to `nsrmdir a/b/c a/b a`\n");
    printf ("  -v, --verbose  output a diagnostic for every directory processed\n");
    printf ("      --help     display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(argc, argv)
     int argc;
     char **argv;
{
  int c;
  char Cnsdir[CA_MAXPATHLEN+1];
  char *path;
  int errflg = 0;
  int vflg = 0;
  int iflg = 0;
  int pflg = 0;
  int hflg = 0;
  int i;
  char *slash;
  char *p;

  Coptions_t longopts[] = {
    { "verbose",                  NO_ARGUMENT, NULL, 'v' },
    { "ignore-fail-on-non-empty", NO_ARGUMENT, NULL, 'i' },
    { "parents",                  NO_ARGUMENT, NULL, 'p' },
    { "help",                     NO_ARGUMENT, &hflg, 1  },
    { NULL,                       0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "vip", longopts, NULL)) != EOF) {
    switch(c) {
    case 'v':
      vflg++;
      break;
    case 'i':
      iflg++;
      break;
    case 'p':
      pflg++;
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
  if (Coptind >= argc) {
    errflg++;
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }
  for (i = Coptind; i < argc; i++) {
    path = argv[i];
    if (*path != '/' && strstr (path, ":/") == NULL) {
      if ((p = getenv (CNS_HOME_ENV)) == NULL ||
          strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
        fprintf (stderr, "%s: %s: invalid path\n", argv[0], path);
        errflg++;
        continue;
      } else
        sprintf (Cnsdir, "%s/%s", p, path);
    } else {
      if (strlen (path) > CA_MAXPATHLEN) {
        fprintf (stderr, "%s: %s: %s\n", argv[0], path,
                 sstrerror(SENAMETOOLONG));
        errflg++;
        continue;
      } else
        strcpy (Cnsdir, path);
    }
    if (pflg) {
      /* remove all but one trailing slash */
      p = Cnsdir + strlen (Cnsdir) - 1;
      while (p > Cnsdir && *p == '/')
	*p-- = '\0';
      Cnsdir[strlen (Cnsdir)] = '/';

      /* loop over all components of the path name */
      while (1) {
	if ((slash = strrchr (Cnsdir, '/')) == NULL)
	    break;
	while (slash > Cnsdir && *slash == '/')
	  --slash;
	slash[1] = 0;
	if (strcmp (Cnsdir, "/") == 0)  /* Cnsdir == "/" */
	  break;
	if (vflg)
	  printf ("%s: removing directory, %s\n", argv[0], Cnsdir);
	if (Cns_rmdir (Cnsdir) < 0) {
	  if (iflg && (serrno == EEXIST)) {
	    break;
	  }
	  fprintf (stderr, "%s: %s: %s\n", argv[0], Cnsdir, (serrno == EEXIST) ?
		   "Directory not empty" : sstrerror(serrno));
	  errflg++;
	  break;
	}
      }
    } else {
      if (vflg)
	printf ("%s: removing directory, %s\n", argv[0], Cnsdir);
      if (Cns_rmdir (Cnsdir) < 0) {
	if (iflg && (serrno == EEXIST)) {
	  continue;
	}
	fprintf (stderr, "%s: %s: %s\n", argv[0], Cnsdir, (serrno == EEXIST) ?
		 "Directory not empty" : sstrerror(serrno));
	errflg++;
	continue;
      }
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}
