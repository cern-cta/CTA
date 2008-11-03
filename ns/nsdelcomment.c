/*
 * Copyright (C) 2000-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsdelcomment - delete a comment associated with a file/directory */
#include <errno.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <stdlib.h>
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
    printf ("Usage: %s PATH\n", name);
    printf ("Delete the comment associated with a file/directory.\n\n");
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
  int errflg = 0;
  char fullpath[CA_MAXPATHLEN+1];
  char *p;
  char *path;
#if defined(_WIN32)
  WSADATA wsadata;
#endif

  if (argc == 2) {
    if (strcmp(argv[1], "--help") == 0)
      usage (0, argv[0]);
  }
  if (argc != 2) {
    usage (USERR, argv[0]);
  }
#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, NS052);
    exit (SYERR);
  }
#endif
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
  if (errflg == 0 && Cns_delcomment (fullpath)) {
    fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
    errflg++;
  }
#if defined(_WIN32)
  WSACleanup();
#endif
  if (errflg)
    exit (USERR);
  exit (0);
}
