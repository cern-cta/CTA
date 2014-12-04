/*
 * Copyright (C) 2000-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsenterclass - define a new file class */
#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "getconfent.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s <--name=CLASSNAME> <--id=CLASSID> [OPTION]... \n", name);
    printf ("Enter a new file class.\n\n");
    printf ("      --gid=CLASSGID            class group gid\n");
    printf ("      --group=CLASSGRP          class group name\n");
    printf ("      --id=CLASSID              the class id\n");
    printf ("      --nbcopies=NUM            specifies the number of copies for a file\n");
    printf ("      --uid=CLASSUID            class user uid\n");
    printf ("      --user=CLASSUSER          class user name\n");
    printf ("  -h, --host=HOSTNAME           specify the name server hostname\n");
    printf ("      --help                    display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}


int main(int argc,
         char **argv)
{
  int c;
  struct Cns_fileclass Cns_fileclass;
  char *dp;
  int errflg = 0;
  int hflg = 0;
  struct group *gr;
  struct passwd *pwd;
  char *server = NULL;

  memset (&Cns_fileclass, 0, sizeof(struct Cns_fileclass));
  Cns_fileclass.nbcopies = 1;

  Coptions_t longopts[] = {
    { "gid",            REQUIRED_ARGUMENT, NULL,  OPT_CLASS_GID   },
    { "group",          REQUIRED_ARGUMENT, NULL,  OPT_CLASS_GROUP },
    { "id",             REQUIRED_ARGUMENT, NULL,  OPT_CLASS_ID    },
    { "name",           REQUIRED_ARGUMENT, NULL,  OPT_CLASS_NAME  },
    { "uid",            REQUIRED_ARGUMENT, NULL,  OPT_CLASS_UID   },
    { "user",           REQUIRED_ARGUMENT, NULL,  OPT_CLASS_USER  },
    { "nbcopies",       REQUIRED_ARGUMENT, NULL,  OPT_NBCOPIES    },
    { "host",           REQUIRED_ARGUMENT, NULL,  'h'             },
    { "help",           NO_ARGUMENT,       &hflg, 1               },
    { NULL,             0,                 NULL,  0               }
  };

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "h:", longopts, NULL)) != EOF) {
    switch (c) {
    case OPT_CLASS_GID:
      Cns_fileclass.gid = strtol (Coptarg, &dp, 10);
      if (*dp != '\0') {
        fprintf (stderr,
                 "invalid class_gid %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_CLASS_GROUP:
      if ((gr = getgrnam (Coptarg)) == NULL) {
        fprintf (stderr,
                 "invalid class_group: %s\n", Coptarg);
        errflg++;
      } else
        Cns_fileclass.gid = gr->gr_gid;
      break;
    case OPT_CLASS_ID:
      if ((Cns_fileclass.classid = strtol (Coptarg, &dp, 10)) <= 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid classid %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_CLASS_NAME:
      if (strlen (Coptarg) > CA_MAXCLASNAMELEN) {
        fprintf (stderr,
                 "class name too long: %s\n", Coptarg);
        errflg++;
      } else
        strcpy (Cns_fileclass.name, Coptarg);
      break;
    case OPT_CLASS_UID:
      Cns_fileclass.uid = strtol (Coptarg, &dp, 10);
      if (*dp != '\0') {
        fprintf (stderr,
                 "invalid class_uid %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_CLASS_USER:
      if ((pwd = getpwnam (Coptarg)) == NULL) {
        fprintf (stderr,
                 "invalid class_user: %s\n", Coptarg);
        errflg++;
      } else
        Cns_fileclass.uid = pwd->pw_uid;
      break;
    case OPT_NBCOPIES:
      if ((Cns_fileclass.nbcopies = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid nbcopies %s\n", Coptarg);
        errflg++;
      }
      break;
    case 'h':
      server = Coptarg;
      setenv(CNS_HOST_ENV, server, 1);
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
  if (Coptind < argc || Cns_fileclass.name[0] == '\0') {
    errflg++;
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }

  if (Cns_enterclass (server, &Cns_fileclass) < 0) {
    fprintf (stderr, "nsenterclass %s: %s\n", Cns_fileclass.name,
             sstrerror(serrno));
    exit (USERR);
  }
  exit (0);
}
