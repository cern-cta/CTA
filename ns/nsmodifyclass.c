/*
 * Copyright (C) 2000-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsmodifyclass - modify an existing file class */
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
    printf ("Usage: %s <--id=CLASSID|--name=CLASSNAME> [OPTION]... \n", name);
    printf ("Modify an existing file class.\n\n");
    printf ("      --gid=CLASSGID            the class is restricted to this group gid\n");
    printf ("      --group=CLASSGRP          the class is restricted to this group name\n");
    printf ("      --id=CLASSID              the class number of the class\n");
    printf ("      --name=CLASSNAME          the name of the class\n");
    printf ("      --nbcopies=NUM            specifies the number of copies for a file\n");
    printf ("      --newname=CLASSNAME       the new name for this class\n");
    printf ("      --uid=CLASSUID            the class is restricted to this user uid\n");
    printf ("      --user=CLASSUSER          the class is restricted to this user name\n");
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
  int classid = 0;
  char *class_name = NULL;
  char *dp;
  int errflg = 0;
  int hflg = 0;
  struct group *gr;
  struct passwd *pwd;
  char *server = NULL;

  memset (&Cns_fileclass, 0xFF, sizeof(struct Cns_fileclass));
  *(Cns_fileclass.name) = '\0';

  Coptions_t longopts[] = {
    { "gid",            REQUIRED_ARGUMENT, NULL,  OPT_CLASS_GID   },
    { "group",          REQUIRED_ARGUMENT, NULL,  OPT_CLASS_GROUP },
    { "id",             REQUIRED_ARGUMENT, NULL,  OPT_CLASS_ID    },
    { "name",           REQUIRED_ARGUMENT, NULL,  OPT_CLASS_NAME  },
    { "uid",            REQUIRED_ARGUMENT, NULL,  OPT_CLASS_UID   },
    { "user",           REQUIRED_ARGUMENT, NULL,  OPT_CLASS_USER  },
    { "nbcopies",       REQUIRED_ARGUMENT, NULL,  OPT_NBCOPIES    },
    { "newname",        REQUIRED_ARGUMENT, NULL,  OPT_NEW_C_NAME  },
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
      if ((classid = strtol (Coptarg, &dp, 10)) <= 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid classid %s\n", Coptarg);
        errflg++;
      } else
        Cns_fileclass.classid = classid;
      break;
    case OPT_CLASS_NAME:
      if (strlen (Coptarg) > CA_MAXCLASNAMELEN) {
        fprintf (stderr,
                 "class name too long: %s\n", Coptarg);
        errflg++;
      } else
        class_name = Coptarg;
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
    case OPT_NEW_C_NAME:
      if (strlen (Coptarg) > CA_MAXCLASNAMELEN) {
        fprintf (stderr,
                 "class name too long: %s\n", Coptarg);
        errflg++;
      } else
        strcpy (Cns_fileclass.name, Coptarg);
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
  if (Coptind < argc || (classid == 0 && class_name == NULL)) {
    errflg++;
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }

  if (Cns_modifyclass (server, classid, class_name, &Cns_fileclass) < 0) {
    char buf[256];
    if (classid) sprintf (buf, "%d", classid);
    if (class_name) {
      if (classid) strcat (buf, ", ");
      else buf[0] = '\0';
      strcat (buf, class_name);
    }
    fprintf (stderr, "nsmodifyclass %s: %s\n", buf,
             (serrno == ENOENT) ? "No such class" : sstrerror(serrno));
    exit (USERR);
  }
  exit (0);
}
