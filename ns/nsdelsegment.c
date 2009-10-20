/*
 * Copyright (C) 1999-2004 by CERN/IT/DM
 * All rights reserved
 */

/* nsdelsegment - utility to delete file segments */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "u64subr.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... PATH...\n", name);
    printf ("Utility to delete file segments.\n\n");
    printf ("  -c, --copyno=COPYNO      specifies which copy of the file should be deleted\n");
    printf ("  -a, --all                delete all segments belonging to the file\n");
    printf ("      --help               display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
#if defined(_WIN32)
  WSACleanup();
#endif
  exit (status);
}

int main(argc, argv)
     int argc;
     char **argv;
{
  int c;
  int i;
  int copyno = -1;
  int errflg = 0;
  int hflg   = 0;
  char *path = NULL;
  char *dp   = NULL;
  char *p    = NULL;
  char filepath[CA_MAXPATHLEN+1];

#if defined(_WIN32)
  WSADATA wsadata;
#endif

  Coptions_t longopts[] = {
    { "copyno",    REQUIRED_ARGUMENT, NULL,  'c' },
    { "all",       NO_ARGUMENT,       NULL,  'a' },
    { "help",      NO_ARGUMENT,       &hflg,  1  },
    { NULL,        0,                 NULL,   0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "c:a", longopts, NULL)) != EOF && !errflg) {
    switch(c) {
    case 'a':
      copyno = 0;
      break;
    case 'c':
      if (((copyno = strtol (Coptarg, &dp, 10)) <= 0) || (*dp != '\0')) {
	fprintf (stderr, "%s: invalid copy number: %s\n", argv[0], Coptarg);
	errflg++;
      }
      break;
    case '?':
    case ':':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (hflg) {
    usage (0, argv[0]);
  }
  if (errflg || Coptind >= argc || copyno == -1) {
    usage (USERR, argv[0]);
  }

#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, NS052);
    exit (SYERR);
  }
#endif

  for (i = Coptind; i < argc; i++) {
    path = argv[i];
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

    if (Cns_delsegbycopyno(filepath, 0, copyno)) {
      fprintf (stderr, "%s: %s: %s\n", argv[0], filepath,
               serrno == SEENTRYNFND ? "no segments found" : sstrerror(serrno));
      errflg++;
    }
  }
#if defined(_WIN32)
  WSACleanup();
#endif
  if (errflg)
    exit (USERR);
  exit (0);
}
