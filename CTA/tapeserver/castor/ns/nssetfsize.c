/*
 * Copyright (C) 1999-2004 by CERN/IT/DM
 * All rights reserved
 */

/* nssetfsize - utility to set the size of a file */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... PATH\n", name);
    printf ("Update the size of a file in the CASTOR name server\n\n");
    printf ("  -x, --size=SIZE    the size of the file in bytes\n");
    printf ("  -r, --reset        reset the file size to 0\n");
    printf ("      --help         display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,
         char **argv)
{
  int errflg = 0;
  int hflg   = 0;
  int xflg   = 0;
  int rflg   = 0;
  int c;
  char *p;
  char *dp   = NULL;
  char *path = NULL;
  char filepath[CA_MAXPATHLEN+1];
  u_signed64 size = 0;
  struct Cns_filestatcs stat;

  Coptions_t longopts[] = {
    { "help",  NO_ARGUMENT,       &hflg,  1  },
    { "size",  REQUIRED_ARGUMENT, NULL,  'x' },
    { "reset", NO_ARGUMENT,       &rflg,  1  },
    { NULL,    0,                 NULL,   0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "x:r", longopts, NULL)) != EOF) {
    switch (c) {
    case 'x':
      xflg++;
      if (((size = strtoull (Coptarg, &dp, 10)) <= 0) || (*dp != '\0') ||
	  (Coptarg[0] == '-') | (errno == ERANGE)) {
	fprintf (stderr, "%s: invalid file size: %s\n", argv[0], Coptarg);
	errflg++;
      }  
      break;
    case 'r':
      rflg++;
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
  if (rflg) {
    size = 0;
    xflg++;
  }
  if (Coptind >= argc) {   /* Too few arguments */
    errflg++;
  } else if (!xflg) {
    fprintf (stderr, "%s: mandatory option -x, --size is missing\n", argv[0]);
    errflg++;
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }
  path = argv[Coptind];
  if (*path != '/' && strstr (path, ":/") == NULL) {
    if ((p = getenv (CNS_HOME_ENV)) == NULL ||
	strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: %s: invalid path\n", argv[0], path);
      errflg++;
    } else
      sprintf (filepath, "%s/%s", p, path);
  } else {
    if (strlen (path) > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: %s: %s\n", argv[0], path,
	       sstrerror(SENAMETOOLONG));
      errflg++;
    } else
      strcpy (filepath, path);
  }

  /* Get the file stat information */
  if (Cns_statcs(filepath, &stat)) {
    fprintf (stderr, "%s: %s: %s\n", argv[0], filepath, sstrerror(serrno));
    errflg++;
  } else {
    /* Update the file size */
    if (Cns_setfsizecs(filepath, NULL, size, 
                       stat.csumtype, stat.csumvalue,
                       stat.mtime, stat.mtime)) {
      fprintf (stderr, "%s: %s: %s\n", argv[0], filepath, sstrerror(serrno));
      errflg++;
    }
  }

  if (errflg)
    exit (USERR);
  exit (0);
}
