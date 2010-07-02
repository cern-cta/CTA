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
    printf ("Usage: %s <--name=CLASSNAME> [OPTION]... \n", name);
    printf ("Modify an existing file class.\n\n");
    printf ("      --gid=CLASSGID            the class is restricted to this group gid\n");
    printf ("      --group=CLASSGRP          the class is restricted to this group name\n");
    printf ("      --id=CLASSID              the class number of the class to be modified\n");
    printf ("      --maxdrives=NUM           specifies the number of drives which can be used\n");
    printf ("                                in parallel for a migration\n");
    printf ("      --maxfilesize=NUM         specifies the maximum file size\n");
    printf ("      --maxsegsize=NUM          specifies the maximum segment size\n");
    printf ("      --migr_interval=NUM       a new migration will be started if at least NUM\n");
    printf ("                                seconds have\n");
    printf ("                                elapsed since the last migration\n");
    printf ("      --minfilesize=NUM         specifies the minimum file size\n");
    printf ("      --mintime=NUM             a file will not be migrated unless at least NUM\n");
    printf ("                                seconds have\n");
    printf ("                                elapsed since the last update\n");
    printf ("      --name=CLASSNAME          the name of the class to be modified\n");
    printf ("      --nbcopies=NUM            specifies the number of copies for a file\n");
    printf ("      --retenp_on_disk=NUM      specifies the maximum retention period (in seconds)\n");
    printf ("                                for a file on disk. The retention period can also be\n");
    printf ("                                set to AS_LONG_AS_POSSIBLE or INFINITE_LIFETIME. If\n");
    printf ("                                zero, the file is purged immediately after migration.\n");
    printf ("                                Default is AS_LONG_AS_POSSIBLE, i.e. purged when\n");
    printf ("                                disk space is needed\n");
    printf ("      --tppools=POOL1:POOL2...  Specifies the tape pools to be used for migration.\n");
    printf ("                                The number of tape pools must be at least as big as\n");
    printf ("                                the number of copies.\n");
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
  char *dp;
  int errflg = 0;
  int hflg = 0;
  struct group *gr;
  int nbtppools;
  char *p = NULL;
  struct passwd *pwd;
  char *q;
  char *server = NULL;

  memset (&Cns_fileclass, 0, sizeof(struct Cns_fileclass));
  Cns_fileclass.retenp_on_disk = AS_LONG_AS_POSSIBLE;

  Coptions_t longopts[] = {
    { "gid",            REQUIRED_ARGUMENT, NULL,  OPT_CLASS_GID   },
    { "group",          REQUIRED_ARGUMENT, NULL,  OPT_CLASS_GROUP },
    { "id",             REQUIRED_ARGUMENT, NULL,  OPT_CLASS_ID    },
    { "name",           REQUIRED_ARGUMENT, NULL,  OPT_CLASS_NAME  },
    { "uid",            REQUIRED_ARGUMENT, NULL,  OPT_CLASS_UID   },
    { "user",           REQUIRED_ARGUMENT, NULL,  OPT_CLASS_USER  },
    { "flags",          REQUIRED_ARGUMENT, NULL,  OPT_FLAGS       },
    { "maxdrives",      REQUIRED_ARGUMENT, NULL,  OPT_MAX_DRV     },
    { "maxfilesize",    REQUIRED_ARGUMENT, NULL,  OPT_MAX_FSZ     },
    { "maxsegsize",     REQUIRED_ARGUMENT, NULL,  OPT_MAX_SSZ     },
    { "migr_interval",  REQUIRED_ARGUMENT, NULL,  OPT_MIGR_INTV   },
    { "minfilesize",    REQUIRED_ARGUMENT, NULL,  OPT_MIN_FSZ     },
    { "mintime",        REQUIRED_ARGUMENT, NULL,  OPT_MIN_TIME    },
    { "nbcopies",       REQUIRED_ARGUMENT, NULL,  OPT_NBCOPIES    },
    { "retenp_on_disk", REQUIRED_ARGUMENT, NULL,  OPT_RETENP_DISK },
    { "tppools",        REQUIRED_ARGUMENT, NULL,  OPT_TPPOOLS     },
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
    case OPT_FLAGS:
      if ((Cns_fileclass.flags = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid flags %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_MAX_DRV:
      if ((Cns_fileclass.maxdrives = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid maxdrives %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_MAX_FSZ:
      if ((Cns_fileclass.max_filesize = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid maxfilesize %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_MAX_SSZ:
      if ((Cns_fileclass.max_segsize = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid maxsegsize %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_MIGR_INTV:
      if ((Cns_fileclass.migr_time_interval = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid migr_interval %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_MIN_FSZ:
      if ((Cns_fileclass.min_filesize = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid minfilesize %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_MIN_TIME:
      if ((Cns_fileclass.mintime_beforemigr = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid mintime %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_NBCOPIES:
      if ((Cns_fileclass.nbcopies = strtol (Coptarg, &dp, 10)) < 0 ||
          *dp != '\0') {
        fprintf (stderr,
                 "invalid nbcopies %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_RETENP_DISK:
      if (strcmp (Coptarg, "AS_LONG_AS_POSSIBLE") == 0)
        Cns_fileclass.retenp_on_disk = AS_LONG_AS_POSSIBLE;
      else if (strcmp (Coptarg, "INFINITE_LIFETIME") == 0)
        Cns_fileclass.retenp_on_disk = INFINITE_LIFETIME;
      else if ((Cns_fileclass.retenp_on_disk =
                strtol (Coptarg, &dp, 10)) < 0 || *dp != '\0') {
        fprintf (stderr,
                 "invalid retenp_on_disk %s\n", Coptarg);
        errflg++;
      }
      break;
    case OPT_TPPOOLS:
      nbtppools = 0;
      p = strtok (Coptarg, ":");
      while (p) {
        if (strlen (p) > CA_MAXPOOLNAMELEN) {
          fprintf (stderr,
                   "invalid tppools %s\n", Coptarg);
          errflg++;
        } else
          nbtppools++;
        if ((p = strtok (NULL, ":"))) *(p - 1) = ':';
      }
      if (errflg) break;
      Cns_fileclass.nbtppools = nbtppools;
      if ((q = calloc (nbtppools, CA_MAXPOOLNAMELEN+1)) == NULL) {
        fprintf (stderr, "nsenterclass %s: %s\n",
                 Cns_fileclass.name, sstrerror(ENOMEM));
        errflg++;
        break;
      }
      Cns_fileclass.tppools = q;
      p = strtok (Coptarg, ":");
      while (p) {
        strcpy (q, p);
        q += (CA_MAXPOOLNAMELEN+1);
        p = strtok (NULL, ":");
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
