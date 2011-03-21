/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/*	vmgrdeletelibrary - delete a tape library definition */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "vmgr_api.h"

int main(int argc,
         char **argv)
{
  int c;
  int errflg = 0;
  static struct Coptions longopts[] = {
    {"name", REQUIRED_ARGUMENT, 0, OPT_LIBRARY_NAME},
    {0, 0, 0, 0}
  };
  char *library_name = NULL;

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
    switch (c) {
    case OPT_LIBRARY_NAME:
      library_name = Coptarg;
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (Coptind < argc || library_name == NULL) {
    errflg++;
  }
  if (errflg) {
    fprintf (stderr, "usage: %s --name library_name\n", argv[0]);
    exit (USERR);
  }

  if (vmgr_deletelibrary (library_name) < 0) {
    fprintf (stderr, "vmgrdeletelibrary %s: %s\n", library_name,
             (serrno == ENOENT) ? "No such library" : sstrerror(serrno));
    exit (USERR);
  }
  exit (0);
}
