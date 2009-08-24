/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsdeleteclass - delete a fileclass definition */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "getconfent.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s <--id=CLASSID|--name=CLASSNAME> [OPTION]...\n", name);
    printf ("Delete a file class from the CASTOR name server.\n\n");
    printf ("      --id=CLASSID      the class number of the class to be deleted\n");
    printf ("      --name=CLASSNAME  the name of the class to be deleted\n");
    printf ("  -h, --host=HOSTNAME   specify the name server hostname\n");
    printf ("      --help            display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(argc, argv)
     int argc;
     char **argv;
{
  int c;
  int classid = 0;
  char *class_name = NULL;
  struct Cns_fileclass Cns_fileclass;
  char *dp;
  int errflg = 0;
  int hflg = 0;
  char *server = NULL;

  memset (&Cns_fileclass, 0, sizeof(struct Cns_fileclass));

  Coptions_t longopts[] = {
    { "id",   REQUIRED_ARGUMENT, NULL,  OPT_CLASS_ID   },
    { "name", REQUIRED_ARGUMENT, NULL,  OPT_CLASS_NAME },
    { "host", REQUIRED_ARGUMENT, NULL,  'h'            },
    { "help", NO_ARGUMENT,       &hflg, 1              },
    { NULL,   0,                 NULL,  0              }
  };

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "h:", longopts, NULL)) != EOF) {
    switch (c) {
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
      class_name = Coptarg;
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

  if (Cns_deleteclass (server, classid, class_name) < 0) {
    char buf[256];
    if (classid) sprintf (buf, "%d", classid);
    if (class_name) {
      if (classid) strcat (buf, ", ");
      else buf[0] = '\0';
      strcat (buf, class_name);
    }
    fprintf (stderr, "nsdeleteclass %s: %s\n", buf,
             (serrno == ENOENT) ? "No such class" : sstrerror(serrno));
    exit (USERR);
  }
  exit (0);
}
