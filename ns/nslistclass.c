/*
 * Copyright (C) 2000-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nslistclass - query a given class or list all existing file classes */
#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "getconfent.h"

int nohdr = 0;
int listentry(struct Cns_fileclass *Cns_fileclass);

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s <--id=CLASSID|--name=CLASSNAME> [OPTION]...\n", name);
    printf ("List existing file class(es).\n\n");
    printf ("      --id=CLASSID      the class number of the class to be displayed\n");
    printf ("      --name=CLASSNAME  the name of the class to be displayed\n");
    printf ("      --nohdr           display each class on one line, removing key value pair\n");
    printf ("                        listing\n");
    printf ("  -h, --host=HOSTNAME   specify the name server hostname\n");
    printf ("      --help            display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,char **argv)
{
  int c;
  int classid = 0;
  char *class_name = NULL;
  struct Cns_fileclass Cns_fileclass;
  char *dp;
  int errflg = 0;
  int hflg = 0;
  int flags;
  Cns_list list;
  struct Cns_fileclass *lp;
  char *server = NULL;

  Coptions_t longopts[] = {
    { "id",    REQUIRED_ARGUMENT, NULL,   OPT_CLASS_ID   },
    { "name",  REQUIRED_ARGUMENT, NULL,   OPT_CLASS_NAME },
    { "nohdr", NO_ARGUMENT,       &nohdr, 1              },
    { "host",  REQUIRED_ARGUMENT, NULL,  'h'             },
    { "help",  NO_ARGUMENT,       &hflg,  1              },
    { NULL,    0,                 NULL,   0              }
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
      }
      break;
    case OPT_CLASS_NAME:
      if (strlen (Coptarg) > CA_MAXCLASNAMELEN) {
        fprintf (stderr,
                 "class name too long: %s\n", Coptarg);
        errflg++;
      } else
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
  if (Coptind < argc)
    errflg++;
  if (errflg) {
    usage (USERR, argv[0]);
  }

  if (classid > 0 || class_name) {
    if (Cns_queryclass (server, classid, class_name, &Cns_fileclass) < 0) {
      char buf[256];
      if (classid) sprintf (buf, "%d", classid);
      if (class_name) {
        if (classid) strcat (buf, ", ");
        else buf[0] = '\0';
        strcat (buf, class_name);
      }
      fprintf (stderr, "nslistclass %s: %s\n", buf,
               (serrno == ENOENT) ? "No such class" : sstrerror(serrno));
      exit (USERR);
    }
    listentry (&Cns_fileclass);
    if (Cns_fileclass.tppools)
      free (Cns_fileclass.tppools);
  } else {
    flags = CNS_LIST_BEGIN;
    while ((lp = Cns_listclass (server, flags, &list)) != NULL) {
      if (flags == CNS_LIST_CONTINUE && ! nohdr)
        printf ("\n");
      listentry (lp);
      flags = CNS_LIST_CONTINUE;
    }
    (void) Cns_listclass (server, CNS_LIST_END, &list);
  }
  exit (0);
}

int listentry(struct Cns_fileclass *Cns_fileclass)
{
  if (nohdr) {
    printf ("%d %s %d", Cns_fileclass->classid, Cns_fileclass->name, Cns_fileclass->nbcopies);
  } else {
    printf ("CLASS_ID %d\n", Cns_fileclass->classid);
    printf ("CLASS_NAME %s\n", Cns_fileclass->name);
    printf ("NBCOPIES %d", Cns_fileclass->nbcopies);
  }
  printf ("\n");
  return (0);
}
