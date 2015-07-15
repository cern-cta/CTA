/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nschown - change directory/file ownership in name server */
#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <ctype.h>
#include <getopt.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

int hflag = 0;
int chowndir (char *dir,uid_t newuid,gid_t newgid);

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... OWNER[:[GROUP]] PATH...\n", name);
    printf ("Change owner and group of a CASTOR directory/file in the name server.\n\n");
    printf ("  -h          if PATH is a symbolic link, changes the ownership of the link itself\n");
    printf ("  -R          recursively change ownership\n");
    printf ("      --help  display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc, char **argv)
{
  int c;
  char *dp;
  int Rflag = 0;
  int errflg = 0;
  char fullpath[CA_MAXPATHLEN+1];
  struct group *gr;
  int i;
  gid_t newgid = 0;
  uid_t newuid = 0;
  char *p;
  char *path;
  struct passwd *pwd;
  struct Cns_filestat statbuf;

  Coptions_t longopts[] = {
    { "help", NO_ARGUMENT, &hflag, 1  },
    { NULL,   0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "hR", longopts, NULL)) != EOF) {
    switch (c) {
    case 'h':
      hflag++;
      break;
    case 'R':
      Rflag++;
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (hflag) {
    usage (0, argv[0]);
  }
  if (errflg || Coptind >= argc - 1) {
    usage (USERR, argv[0]);
  }
  p = strtok (argv[Coptind], ":.");
  if (NULL == p) {
    fprintf (stderr, "invalid empty user\n");
    exit (USERR);
  }
  if (isdigit (*p)) {
    newuid = strtol (p, &dp, 10);
    if (*dp != '\0') {
      fprintf (stderr, "invalid user: %s\n", p);
      errflg++;
    }
  } else {
    if ((pwd = getpwnam (p))) {
      newuid = pwd->pw_uid;
    } else {
      fprintf (stderr, "invalid user: %s\n", p);
      errflg++;
    }
  }
  if ((p = strtok (NULL, ":."))) {
    if (isdigit (*p)) {
      newgid = strtol (p, &dp, 10);
      if (*dp != '\0') {
        fprintf (stderr, "invalid group: %s\n", p);
        errflg++;
      }
    } else {
      if ((gr = getgrnam (p))) {
        newgid = gr->gr_gid;
      } else {
        fprintf (stderr, "invalid group: %s\n", p);
        errflg++;
      }
    }
  } else
    newgid = -1;
  if (errflg)
  exit (USERR);
  for (i = Coptind+1; i < argc; i++) {
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
    if (Cns_lstat (fullpath, &statbuf) < 0) {
      fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
      errflg++;
      continue;
    }
    if (Rflag && (statbuf.filemode & S_IFDIR))
      if (chowndir (fullpath, newuid, newgid))
        errflg++;
    if (hflag) {
      if (Cns_lchown (fullpath, newuid, newgid)) {
        fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
        errflg++;
      }
    } else {
      if (Cns_chown (fullpath, newuid, newgid)) {
        fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
        errflg++;
      }
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}

int chowndir (char *dir, uid_t newuid, gid_t newgid)
{
  char curdir[CA_MAXPATHLEN+1];
  struct dirlist {
    char *d_name;
    struct dirlist *next;
  };
  Cns_DIR *dirp;
  struct dirlist *dlc;  /* pointer to current directory in the list */
  struct dirlist *dlf = NULL; /* pointer to first directory in the list */
  struct dirlist *dll = NULL; /* pointer to last directory in the list */
  struct Cns_direnstat *dxp;
  char fullpath[CA_MAXPATHLEN+1];
  int errflg = 0;

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
        fprintf (stderr, "%s: %s\n", dxp->d_name,
                 sstrerror(serrno));
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
      if (hflag) {
        if (Cns_lchown (fullpath, newuid, newgid) < 0) {
          fprintf (stderr, "%s: %s\n", fullpath,
                   sstrerror(serrno));
          errflg++;
        }
      } else {
        if (Cns_chown (fullpath, newuid, newgid) < 0) {
          fprintf (stderr, "%s: %s\n", fullpath,
                   sstrerror(serrno));
          errflg++;
        }
      }
    }
  }
  (void) Cns_closedir (dirp);
  while (dlf) {
    sprintf (curdir, "%s/%s", dir, dlf->d_name);
    if (chowndir (curdir, newuid, newgid) < 0) {
      errflg++;
    }
    free (dlf->d_name);
    dlc = dlf;
    dlf = dlf->next;
    free (dlc);
  }
  if (Cns_chdir ("..") < 0)
    return (-1);
  if (hflag) {
    if (Cns_lchown (dir, newuid, newgid) < 0) {
      fprintf (stderr, "%s: %s\n", fullpath,
               sstrerror(serrno));
      errflg++;
    }
  } else {
    if (Cns_lchown (dir, newuid, newgid) < 0) {
      fprintf (stderr, "%s: %s\n", dir, sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    return (-1);
  return (0);
}
