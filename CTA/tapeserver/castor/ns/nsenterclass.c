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
    printf ("      --name=CLASSNAME          the name of the class\n");
    printf ("      --id=CLASSID              the class id\n");
    printf ("      --nbcopies=NUM            specifies the number of copies for a file\n");
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
  char *server = NULL;

  memset (&Cns_fileclass, 0, sizeof(struct Cns_fileclass));
  Cns_fileclass.nbcopies = 1;

  Coptions_t longopts[] = {
    { "id",             REQUIRED_ARGUMENT, NULL,  OPT_CLASS_ID    },
    { "name",           REQUIRED_ARGUMENT, NULL,  OPT_CLASS_NAME  },
    { "nbcopies",       REQUIRED_ARGUMENT, NULL,  OPT_NBCOPIES    },
    { "host",           REQUIRED_ARGUMENT, NULL,  'h'             },
    { "help",           NO_ARGUMENT,       &hflg, 1               },
    { NULL,             0,                 NULL,  0               }
  };

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "h:", longopts, NULL)) != EOF) {
    switch (c) {
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
