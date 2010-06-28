/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsmkdir - make name server directory entries */
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
    printf ("Usage: %s [OPTION] DIRECTORY...\n", name);
    printf ("Create the DIRECTORY(ies), if they do not already exist..\n\n");
    printf ("  -m, --mode=MODE  specifies the mode to be used\n");
    printf ("  -p, --parents    create all the non-existing parent directories first\n");
    printf ("      --help       display this help and exit\n\n");
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
  char *dir;
  char *dp;
  char *endp;
  int errflg = 0;
  int hflg = 0;
  int i;
  mode_t mask;
  int mflag = 0;
  mode_t mode;
  char *p;
  int pflag = 0;
  struct Cns_filestat statbuf;

  Coptions_t longopts[] = {
    { "mode",    REQUIRED_ARGUMENT, NULL, 'm' },
    { "parents", NO_ARGUMENT,       NULL, 'p' },
    { "help",    NO_ARGUMENT,       &hflg, 1  },
    { NULL,      0,                 NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  mode = 0777;
  while ((c = Cgetopt_long (argc, argv, "m:p", longopts, NULL)) != EOF) {
    switch (c) {
    case 'm':
      mflag++;
      mode = strtol (Coptarg, &dp, 8);
      if (*dp != '\0') {
        fprintf (stderr, "invalid value for option -m\n");
        errflg++;
      }
      break;
    case 'p':
      pflag++;
      mask = Cns_umask (0);
      Cns_umask (mask);
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
  if (Coptind >= argc) {
    errflg++;
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }
  for (i = Coptind; i < argc; i++) {
    dir = argv[i];
    if (*dir != '/' && strstr (dir, ":/") == NULL) {
      if ((p = getenv (CNS_HOME_ENV)) == NULL ||
          strlen (p) + strlen (dir) + 1 > CA_MAXPATHLEN) {
        fprintf (stderr, "cannot create %s: invalid path\n",
                 dir);
        errflg++;
        continue;
      } else
        sprintf (Cnsdir, "%s/%s", p, dir);
    } else {
      if (strlen (dir) > CA_MAXPATHLEN) {
        fprintf (stderr, "cannot create %s: %s\n", dir,
                 sstrerror(SENAMETOOLONG));
        errflg++;
        continue;
      } else
        strcpy (Cnsdir, dir);
    }
    if (! pflag) {
      if ((c = Cns_mkdir (Cnsdir, mode)) < 0) {
        fprintf (stderr, "cannot create %s: %s\n", dir,
                 sstrerror(serrno));
        errflg++;
        continue;
      }
    } else {
      if (Cns_lstat (Cnsdir, &statbuf) == 0) {
        if ((statbuf.filemode & S_IFDIR) == 0) {
          fprintf (stderr, "cannot create %s: %s\n",
                   dir, strerror(EEXIST));
          errflg++;
        }
        continue;
      }
      endp = strrchr (Cnsdir, '/');
      p = endp;
      while (p > Cnsdir) {
        *p = '\0';
        if (Cns_access (Cnsdir, F_OK) == 0) break;
        p = strrchr (Cnsdir, '/');
      }
      /* make sure that mask allow write/execute for owner */
      if (mask & 0300)
        Cns_umask (mask & ~ 0300);
      while (p <= endp) {
        *p = '/';
        if (p == endp && mask & 0300)
          Cns_umask (mask);
        c = Cns_mkdir (Cnsdir, 0777);
        if (c < 0 && serrno != EEXIST) {
          fprintf (stderr, "cannot create %s: %s\n", dir,
                   sstrerror(serrno));
          errflg++;
          p = endp + 1; /* exit from the loop */
          continue;
        }
        p += strlen (p);
      }
    }
    if (c == 0 /* directory successfully created */
        && mflag) {
      /* must set requested mode + possible S_ISGID */
      if (Cns_stat (Cnsdir, &statbuf) < 0) {
        fprintf (stderr, "cannot stat %s: %s\n",
                 dir, sstrerror(serrno));
        errflg++;
        continue;
      }
      if (Cns_chmod (Cnsdir, (mode | (statbuf.filemode & S_ISGID))) < 0) {
        fprintf (stderr, "cannot chmod %s: %s\n",
                 dir, sstrerror(serrno));
        errflg++;
        continue;
      }
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}
