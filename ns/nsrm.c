/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsrm - remove name server directory/file entries */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

int errflg = 0;
int fflag  = 0;
int iflag  = 0;
int rflag  = 0;

int isyes();
int removepath(char *path);
int removedir (char *dir);

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... FILE...\n", name);
    printf ("Remove files or directories.\n\n");
    printf ("  -f, --force          ignore nonexistent files or directories\n");
    printf ("  -i, --interactive    prompt before any removal\n");
    printf ("  -r, -R, --recursive  remove the contents of directories recursively\n");
    printf ("          --help       display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,
         char **argv)
{
  int c;
  int i;
  int hflg = 0;
  char *path;

  Coptions_t longopts[] = {
    { "recursive",   NO_ARGUMENT, NULL, 'r' },
    { "interactive", NO_ARGUMENT, NULL, 'i' },
    { "force",       NO_ARGUMENT, NULL, 'f' },
    { "help",        NO_ARGUMENT, &hflg, 1  },
    { NULL,          0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;

  while ((c = Cgetopt_long (argc, argv, "fiRr", longopts, NULL)) != EOF) {
    switch (c) {
    case 'f':
      fflag++;
      break;
    case 'i':
      iflag++;
      break;
    case 'R':
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
  if (errflg || Coptind >= argc) {
    usage (USERR, argv[0]);
  }
  /* Scan the items to remove */
  for (i = Coptind; i < argc; i++) {
    path = argv[i];

    /* Actually remove the item */
    int returnCode = removepath(path);

    /* Print errors if any */
    if (returnCode && (serrno != ENOENT || fflag == 0)) {
      fprintf (stderr, "%s: %s\n", path,
               sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}

int removepath(char *path)
{
	char *p;
	char fullpath[CA_MAXPATHLEN+1];
	struct Cns_filestat statbuf;
	int returnCode;

	/* Expand the path */
	if (path[0] != '/' && strstr (path, ":/") == NULL) {
		if ((p = getenv (CNS_HOME_ENV)) == NULL || strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
			fprintf (stderr, "%s: invalid path\n", path);
			errflg++;
			returnCode = USERR;
		} else {
			sprintf (fullpath, "%s/%s", p, path);
		}
	} else {
		if (strlen (path) > CA_MAXPATHLEN) {
			fprintf (stderr, "%s: %s\n", path,
					 sstrerror(SENAMETOOLONG));
			errflg++;
			returnCode = USERR;
		} else strcpy (fullpath, path);
	}

	/* What are we going to remove? */
	if (!errflg && (returnCode = Cns_lstat (fullpath, &statbuf)) == 0) {
		/* a directory */
		if (statbuf.filemode & S_IFDIR) {
			if (rflag) {
				if (! fflag && Cns_access (fullpath, W_OK) && isatty (fileno (stdin))) {
					printf ("override write protection for %s? ", fullpath);
					if (!isyes()) return (0);
				} else if (iflag) {
					printf ("remove files in %s? ", fullpath);
					if (!isyes()) return (0);
				}

				/* If the directory is empty just remove it right away */
				if (statbuf.nlink == 0) {
					returnCode = Cns_rmdir(fullpath);
					if (returnCode) {
						fprintf (stderr, "%s: %s\n", fullpath, (serrno == EEXIST) ?
									"Directory not empty" : sstrerror(serrno));
					    errflg++;
					}
				/* ...otherwise attempt to access it and remove all contained objects */
				} else {
					returnCode = removedir(fullpath);
				}
			} else {
				serrno = EISDIR;
				returnCode = -1;
			}
		/* a file */
		} else {
			if ((statbuf.filemode & S_IFLNK) != S_IFLNK
					&& ! fflag
					&& Cns_access (fullpath, W_OK)
					&& isatty (fileno (stdin))) {
				printf ("override write protection for %s? ", fullpath);
				returnCode = (isyes() ? Cns_unlink (fullpath) : 0);
			} else if (iflag) {
				printf ("remove %s? ", fullpath);
				returnCode = (isyes() ? Cns_unlink (fullpath) : 0);
			} else {
				returnCode = Cns_unlink (fullpath);
			}
		}
	}
	return returnCode;
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

/* Recursive procedure to descend into the directory and remove all elements one by one */
int removedir (char *dir)
{
  char curdir[CA_MAXPATHLEN+1];
  struct dirlist {
    char *d_name;
    struct dirlist *next;
  };
  Cns_DIR *dirp;
  struct dirlist *dlc;  /* pointer to current directory in the list */
  struct dirlist *dlf = NULL; /* pointer to first directory in the list */
  struct dirlist *dll;  /* pointer to last directory in the list */
  struct Cns_direnstat *dxp;
  char fullpath[CA_MAXPATHLEN+1];

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
      dlc->next = 0;
      if (dlf == NULL)
        dlf = dlc;
      else
        dll->next = dlc;
      dll = dlc;
    } else {
      sprintf (fullpath, "%s/%s", dir, dxp->d_name);
      if ((dxp->filemode & S_IFLNK) != S_IFLNK &&
          ! fflag && Cns_access (fullpath, W_OK) &&
          isatty (fileno (stdin))) {
        printf ("override write protection for %s? ", fullpath);
        if (! isyes())
          continue;
      } else if (iflag) {
        printf ("remove %s? ", fullpath);
        if (! isyes())
          continue;
      }
      if (Cns_unlink (dxp->d_name)) {
        fprintf (stderr, "%s/%s: %s\n", dir,
                 dxp->d_name, sstrerror(serrno));
        errflg++;
      }
    }
  }
  (void) Cns_closedir (dirp);
  while (dlf) {
    sprintf (curdir, "%s/%s", dir, dlf->d_name);
    if (removepath (curdir) < 0)
      fprintf (stderr, "%s: %s\n", curdir, sstrerror(serrno));
    free (dlf->d_name);
    dlc = dlf;
    dlf = dlf->next;
    free (dlc);
  }
  if (Cns_chdir ("..") < 0)
    return (-1);
  if (iflag) {
    printf ("remove %s? ", dir);
    if (! isyes())
      return (0);
  }
  if (Cns_rmdir (dir)) {
    fprintf (stderr, "%s: %s\n", dir, (serrno == EEXIST) ?
             "Directory not empty" : sstrerror(serrno));
    errflg++;
  }
  return (0);
}
