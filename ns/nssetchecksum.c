/*
 * Copyright (C) 2004 by CERN/IT/DM
 * All rights reserved
 */

/* nssetchecksum - utility to set the checksum of a file in the name server */
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

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... PATH\n", name);
    printf ("Set or reset a checksum for a file.\n\n");
    printf ("  -n, --name=CKSUMNAME  the name of the checksum to be stored. E.g. adler32\n");
    printf ("  -k, --checksum=CKSUM  the value of the checksum to be stored\n");
    printf ("      --clear           remove the checksum value\n");
    printf ("      --help            display this help and exit\n\n");
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
  int errflg = 0;
  int hflg   = 0;
  int clear  = 0;
  char *path = NULL;
  char *dp   = NULL;
  char *p;
  char filepath[CA_MAXPATHLEN+1];

  unsigned int checksum = 0;
  char chksumvalue[CA_MAXCKSUMLEN+1];
  char chksumname[CA_MAXCKSUMNAMELEN+1];

  Coptions_t longopts[] = {
    { "checksum",  REQUIRED_ARGUMENT, NULL,   'k' },
    { "clear",     NO_ARGUMENT,       &clear,  1  },
    { "name",      REQUIRED_ARGUMENT, NULL,   'n' },
    { "help",      NO_ARGUMENT,       &hflg,   1  },
    { NULL,        0,                 NULL,    0  }
  };

  Coptind = 1;
  Copterr = 1;
  chksumvalue[0] = chksumname[0] = '\0';
  while ((c = Cgetopt_long (argc, argv, "k:n:", longopts, NULL)) != EOF && !errflg) {
    switch (c) {
    case 'k':
      if (((checksum = strtoul (Coptarg, &dp, 16)) < 0) || (*dp != '\0')) {
        fprintf (stderr, "%s: invalid checksum: %s\n", argv[0], Coptarg);
        errflg++;
      }
      if (strlen(Coptarg) > CA_MAXCKSUMLEN) {
        fprintf (stderr, "%s: checksum value: %s exceeds %d characters in length\n",
                 argv[0], Coptarg, CA_MAXCKSUMLEN);
        errflg++;
      }
      strncpy(chksumvalue, Coptarg, CA_MAXCKSUMLEN);
      chksumvalue[CA_MAXCKSUMLEN] = '\0';
      break;
    case 'n':
      if (strlen(Coptarg) > CA_MAXCKSUMNAMELEN) {
        fprintf (stderr, "%s: checksum name: %s exceeds %d characters in length\n",
                 argv[0], Coptarg, CA_MAXCKSUMNAMELEN);
        errflg++;
      }
      strncpy(chksumname, Coptarg, CA_MAXCKSUMNAMELEN);
      chksumname[CA_MAXCKSUMLEN] = '\0';
      if (strcmp(chksumname, "AD") == 0 ||
          strcmp(chksumname, "adler32") == 0) {
        if (strcmp(chksumname, "adler32") == 0)
          strcpy(chksumname, "PA");
      } else {
        fprintf (stderr, "%s: invalid checksum name: %s\n", argv[0], Coptarg);
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
  if (errflg || Coptind >= argc) {
    errflg++;
  } else if (clear && !errflg) {
    if (chksumname[0] != '\0') {
      fprintf (stderr, "%s: option -n, --name cannot be specified with --clear\n", argv[0]);
      errflg++;
    } else if (chksumvalue[0] != '\0') {
      fprintf (stderr, "%s: option -k, --checksum cannot be specified with --clear\n", argv[0]);
      errflg++;
    }
  } else if (!clear && ((chksumname[0] == '\0') || (chksumvalue[0] == '\0'))) {
    fprintf (stderr, "%s: mandatory option -n, --name and/or -k, --checksum are missing\n", argv[0]);
    errflg++;
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }
#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, NS052);
    exit (SYERR);
  }
#endif
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
  if (!errflg) {
    if (clear) {
      chksumname[0] = '\0';
      chksumvalue[0] = '\0';
    }
    if (Cns_updatefile_checksum(filepath, chksumname, chksumvalue)) {
      fprintf (stderr, "%s: %s: %s\n", argv[0], filepath, sstrerror(serrno));
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
