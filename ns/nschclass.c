/*
 * Copyright (C) 2000-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nschclass - change class on a directory in name server */
#include <errno.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctype.h>
#include <getopt.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

int errflg;
int iflag;
int rflag;

int chdirclass (char *dir,int oldclass,int newclass,char *newclass_name);

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... [CLASSID|CLASSNAME] DIRECTORY...\n", name);
    printf ("Change the class of a CASTOR directory in the name server.\n\n");
    printf ("  -i, --interactive  request acknowledge before changing each individual entry\n");
    printf ("  -r, --recursive    the class is changed on the directories, not on the existing\n");
    printf ("                     regular files\n");
    printf ("      --help         display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,char **argv)
{
  int c;
  char *dp;
  char fullpath[CA_MAXPATHLEN+1];
  int i;
  int hflg = 0;
  char *class_arg;
  int newclass = 0;
  char *newclass_name = NULL;
  int oldclass = 0;
  char *p;
  char *path;
  struct Cns_filestat statbuf;

  Coptions_t longopts[] = {
    { "interactive", NO_ARGUMENT, NULL, 'i' },
    { "recursive",   NO_ARGUMENT, NULL, 'r' },
    { "help",        NO_ARGUMENT, &hflg, 1  },
    { NULL,          0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "ir", longopts, NULL)) != EOF) {
    switch (c) {
    case 'i':
      iflag++;
      break;
    case 'r':
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
  class_arg = argv[Coptind];
  if (isdigit (*class_arg)) { /* numeric class */
    newclass = strtol (class_arg, &dp, 10);
    if (*dp != '\0') { /* may be a name starting with a digit */
      newclass = 0;
      newclass_name = class_arg;
    }
  } else
    newclass_name = class_arg;
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
    if (iflag) {
      if (Cns_stat (fullpath, &statbuf) < 0)
        return (-1);
      oldclass = statbuf.fileclass;
    }
    if (chdirclass (fullpath, oldclass, newclass, newclass_name)) {
      fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
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

int chdirclass (char *dir,int oldclass,int newclass,char *newclass_name)
{
  int c;
  int classtobechanged;
  char curdir[CA_MAXPATHLEN+1];
  struct dirlist {
    char *d_name;
    int oldclass;
    struct dirlist *next;
  };
  Cns_DIR *dirp;
  struct dirlist *dlc;  /* pointer to current directory in the list */
  struct dirlist *dlf = NULL; /* pointer to first directory in the list */
  struct dirlist *dll;  /* pointer to last directory in the list */
  struct Cns_direnstat *dxp;

  classtobechanged = 1;
  if (iflag) {
    printf ("%s class %d, change? ", dir, oldclass);
    if (! isyes())
      classtobechanged = 0;
  }
  if (classtobechanged) {
    c = Cns_chclass (dir, newclass, newclass_name);
    if (c) {
      fprintf (stderr, "%s: %s\n", dir, sstrerror(serrno));
      errflg++;
    }
  }
  if (! rflag)
    return (0);

  if ((dirp = Cns_opendir (dir)) == NULL)
    return (-1);

  if (Cns_chdir (dir) < 0)
    return (-1);
  while ((dxp = Cns_readdirx (dirp)) != NULL) {
    if (dxp->filemode & S_IFDIR) {
      if ((dlc = (struct dirlist *)
           malloc (sizeof(struct dirlist))) == NULL ||
          (dlc->d_name = strdup (dxp->d_name)) == NULL) {
        serrno = errno;
        return (-1);
      }
      dlc->oldclass = dxp->fileclass;
      dlc->next = 0;
      if (dlf == NULL)
        dlf = dlc;
      else
        dll->next = dlc;
      dll = dlc;
    }
  }
  (void) Cns_closedir (dirp);
  while (dlf) {
    sprintf (curdir, "%s/%s", dir, dlf->d_name);
    if (chdirclass (curdir, dlf->oldclass, newclass, newclass_name) < 0) {
      fprintf (stderr, "%s: %s\n", curdir, sstrerror(serrno));
      errflg++;
    }
    free (dlf->d_name);
    dlc = dlf;
    dlf = dlf->next;
    free (dlc);
  }
  if (Cns_chdir ("..") < 0)
    return (-1);
  return (0);
}
