/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*  vmgrmodifytape - modify an existing tape volume */
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "vmgr_api.h"

int main(int argc,
         char **argv)
{
  int c;
  char *density = NULL;
  char *dp;
  int errflg = 0;
  char *lbltype = NULL;
  char *library = NULL;
  static struct Coptions longopts[] = {
    {"library", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
    {"manufacturer", REQUIRED_ARGUMENT, 0, OPT_MANUFACTURER},
    {"pool", REQUIRED_ARGUMENT, 0, 'P'},
    {"sn", REQUIRED_ARGUMENT, 0, OPT_SN},
    {"status", REQUIRED_ARGUMENT, 0, OPT_STATUS},
    {0, 0, 0, 0}
  };
  char *manufacturer = NULL;
  char *p;
  char *pool_name = NULL;
  char *sn = NULL;
  int status = -1;
  char statusa[50];
  char *vid = NULL;
  char *vsn = NULL;

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "d:l:P:V:v:", longopts, NULL)) != EOF) {
    switch (c) {
    case OPT_LIBRARY_NAME:
      library = Coptarg;
      break;
    case OPT_MANUFACTURER:
      manufacturer = Coptarg;
      break;
    case OPT_SN:
      sn = Coptarg;
      break;
    case OPT_STATUS:
      if (isdigit (*Coptarg)) {
        if ((status = strtol (Coptarg, &dp, 10)) < 0 ||
            *dp != '\0') {
          fprintf (stderr,
                   "invalid status %s\n", Coptarg);
          errflg++;
        }
      } else {
        if (strlen (Coptarg) >= sizeof (statusa)) {
          fprintf (stderr,
                   "invalid status %s\n", Coptarg);
          errflg++;
          break;
        }
        status = 0;
        strcpy (statusa, Coptarg);
        p = strtok (statusa, "|");
        while (p) {
          if (strcmp (p, "DISABLED") == 0)
            status |= DISABLED;
          else if (strcmp (p, "EXPORTED") == 0)
            status |= EXPORTED;
          else if (strcmp (p, "TAPE_FULL") == 0)
            status |= TAPE_FULL;
          else if (strcmp (p, "FULL") == 0)
            status |= TAPE_FULL;
          else if (strcmp (p, "TAPE_RDONLY") == 0)
            status |= TAPE_RDONLY;
          else if (strcmp (p, "RDONLY") == 0)
            status |= TAPE_RDONLY;
          else if (strcmp (p, "ARCHIVED") == 0)
            status |= ARCHIVED;
          else {
            fprintf (stderr,
                     "invalid status %s\n", Coptarg);
            errflg++;
            break;
          } // else
          p = strtok (NULL, "|");
        } // while (p)
      } // else
      break;
    case 'd':
      density = Coptarg;
      break;
    case 'l':
      lbltype = Coptarg;
      break;
    case 'P':
      pool_name = Coptarg;
      break;
    case 'V':
      vid = Coptarg;
      break;
    case 'v':
      vsn = Coptarg;
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    } // switch (c)
  } // while ((c = Cgetopt_
  if (Coptind < argc) {
    errflg++;
  }
  if (vid == NULL) {
    fprintf (stderr, "no vid\n");
    errflg++;
  }

  if (errflg) {
    fprintf (stderr, "usage: %s %s%s%s", argv[0],
             "[-d density] [-l lbltype] [-P pool_name] -V vid [-v vsn]\n",
             "[--library library] [--ma manufacturer] [--pool pool_name]\n",
             "[--sn serial_number] [--st status]\n");
    exit (USERR);
  }

  if (vmgr_modifytape (vid, vsn, library, density, lbltype,
                       manufacturer, sn, pool_name, status) < 0) {
    if(serrno == EEXIST){
      fprintf (stderr, "vmgrmodifytape %s:"
               " There are physical files on the tape."
               " Please reclaim the tape before modifying the density or the label.\n",
               vid);
    }
    else {
      fprintf (stderr, "vmgrmodifytape %s: %s\n", vid,
               (serrno == ENOENT) ? "No such tape" : sstrerror(serrno));
    }
    exit (USERR);
  }
  exit (0);
}
