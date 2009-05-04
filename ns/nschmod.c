/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nschmod - change directory/file permissions in name server */
#include <errno.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <stdlib.h>
#include <ctype.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s MODE PATH\n", name);
    printf ("Change access mode of a CASTOR file/directory in the name server.\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
#if defined(_WIN32)
  WSACleanup();
#endif
  exit (status);
}

int main(int argc,char **argv)
{
  int absmode = 0;
  char *dp;
  int errflg = 0;
  char fullpath[CA_MAXPATHLEN+1];
  int i;
  char *mode_arg;
  mode_t newmode;
  char *p;
  char *path;
#if defined(_WIN32)
  WSADATA wsadata;
#endif

  if (argc == 2) {
    if (strcmp(argv[1], "--help") == 0)
      usage (0, argv[0]);
  }
  if (argc < 3) {
    usage (USERR, argv[0]);
  }
  mode_arg = argv[1];
  if (isdigit (*mode_arg)) { /* absolute mode */
    newmode = strtol (mode_arg, &dp, 8);
    if (*dp != '\0' || newmode > 07777) {
      fprintf (stderr, "invalid mode: %s\n", mode_arg);
#if defined(_WIN32)
      WSACleanup();
#endif
      exit (USERR);
    }
    absmode = 1;
  } else { /* mnemonic */
    fprintf (stderr, "symbolic modes not supported yet\n");
    exit (USERR);
  }
#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, NS052);
    exit (SYERR);
  }
#endif
  for (i = 2; i < argc; i++) {
    path = argv[i];
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
    if (Cns_chmod (fullpath, newmode)) {
      fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
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
