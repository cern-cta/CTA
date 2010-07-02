/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nschmod - change directory/file permissions in name server */
#include <errno.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>
#include <ctype.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION] MODE PATH...\n", name);
    printf ("Change access mode of a CASTOR file/directory in the name server.\n\n");
    printf ("  -R, --recursive  recursively change permissions\n");
    printf ("      --help       display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int chmoddir (char *dir,
              mode_t newmode)
{
  char curdir[CA_MAXPATHLEN+1];
  struct dirlist {
    char *d_name;
    struct dirlist *next;
  };
  Cns_DIR *dirp;
  struct dirlist *dlc;        /* pointer to current directory in the list */
  struct dirlist *dlf = NULL; /* pointer to first directory in the list   */
  struct dirlist *dll;        /* pointer to last directory in the list    */
  struct Cns_direnstat *dxp;
  char fullpath[CA_MAXPATHLEN+1];
  int errflg = 0;

  if (Cns_chmod (dir, newmode) < 0) {
    fprintf (stderr, "%s: %s\n", dir, sstrerror(serrno));
    return (-1);
  }
  if ((dirp = Cns_opendir (dir)) == NULL) {
    fprintf (stderr, "%s: %s\n", dir, sstrerror(serrno));
    return (-1);
  }  
  if (Cns_chdir (dir) < 0) {
    fprintf (stderr, "%s: %s\n", dir, sstrerror(serrno));
    return (-1);
  }
  while ((dxp = Cns_readdirx (dirp)) != NULL) {
    if (dxp->filemode & S_IFDIR) {
      if ((dlc = (struct dirlist *)
           malloc (sizeof(struct dirlist))) == NULL ||
          (dlc->d_name = strdup (dxp->d_name)) == NULL) {
        serrno = errno;
        return (-1);
      }
      dlc->next = 0;
      if (dlf == NULL)
        dlf = dlc;
      else
        dll->next = dlc;
      dll = dlc;
    } else {
      sprintf (fullpath, "%s/%s", dir, dxp->d_name);
      if (Cns_chmod (fullpath, newmode) < 0) {
        fprintf (stderr, "%s: %s\n", fullpath, sstrerror(serrno));
        errflg++;
      }
    }
  }
  (void) Cns_closedir (dirp);
  while (dlf) {
    sprintf (curdir, "%s/%s", dir, dlf->d_name);
    if (chmoddir (curdir, newmode) < 0) {
      errflg++;
    }
    free (dlf->d_name);
    dlc = dlf;
    dlf = dlf->next;
    free (dlc);
  }
  if (Cns_chdir ("..") < 0)
    return (-1);

  if (errflg)
    return (-1);
  return (0);
}

int main(int argc,char **argv)
{
  int absmode = 0;
  char *dp;
  int c = 0;
  int errflg = 0;
  char fullpath[CA_MAXPATHLEN+1];
  int i;
  int hflg = 0;
  int rflag = 0;
  char *mode_arg;
  mode_t newmode;
  char *p;
  char *path;
  struct Cns_filestat statbuf;

  Coptions_t longopts[] = {
    { "recursive",   NO_ARGUMENT, NULL, 'r' },
    { "help",        NO_ARGUMENT, &hflg, 1  },
    { NULL,          0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "R", longopts, NULL)) != EOF) {
    switch (c) {
    case 'R':
      rflag++;
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
  if (errflg || Coptind >= argc - 1) {
    usage (USERR, argv[0]);
  }
  mode_arg = argv[Coptind];
  if (isdigit (*mode_arg)) { /* absolute mode */
    newmode = strtol (mode_arg, &dp, 8);
    if (*dp != '\0' || newmode > 07777) {
      fprintf (stderr, "invalid mode: %s\n", mode_arg);
      exit (USERR);
    }
    absmode = 1;
  } else { /* mnemonic */
    fprintf (stderr, "symbolic modes not supported yet\n");
    exit (USERR);
  }
  for (i = Coptind + 1; i < argc; i++) {
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
    if (rflag) {
      if ((c = Cns_lstat (fullpath, &statbuf)) == 0) {
        if (statbuf.filemode & S_IFDIR) {
          if (chmoddir (fullpath, newmode) < 0) {
            errflg++;
          }
        } else {
          c = Cns_chmod (fullpath, newmode);
        }
      }
    } else {
      c = Cns_chmod (fullpath, newmode);
    }
    if (c < 0) {
      fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}
