/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*      reclaim - reset information concerning a volume */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "h/Cns_api.h"
#include "h/Cns.h"
#include "h/getconfent.h"
#include "h/serrno.h"
#include "h/vmgr_api.h"

extern  char  *optarg;
extern  int  optind;

int main(int argc,
         char **argv)
{
  int c = 0;
  struct Cns_direntape *dtp = NULL;
  int errflg = 0;
  int flags = 0;
  char *const host = getconfent (CNS_SCE, "HOST", 0);
  Cns_list list;
  char p_stat[9];
  struct vmgr_tape_info_byte_u64 tape_info;
  FILE *tmpfile();
  char *vid = NULL;

  /* Abort with an appropriate error message if the user did not provide the */
  /* name-server host name in castor.conf                                    */
  if (!host) {
    fprintf (stderr, "Error: CASTOR name-server host name must be provided in"
      " castor.conf\n");
    exit (USERR);
  }

  while ((c = getopt (argc, argv, "V:")) != EOF) {
    switch (c) {
    case 'V':
      vid = optarg;
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (optind < argc || ! vid) {
    errflg++;
  }
  if (errflg) {
    fprintf (stderr, "usage: %s -V vid\n", argv[0]);
    exit (USERR);
  }

  /* check if the volume is FULL */
  if (vmgr_querytape_byte_u64 (vid, 0, &tape_info, NULL) < 0) {
    fprintf (stderr, "reclaim %s: %s\n", vid,
        (serrno == ENOENT) ? "No such volume" : sstrerror(serrno));
    exit (USERR);
  }
  if ((tape_info.status & TAPE_FULL) == 0) {
    if (tape_info.status == 0) strcpy (p_stat, "FREE");
    else if (tape_info.status & TAPE_BUSY) strcpy (p_stat, "BUSY");
    else if (tape_info.status & TAPE_RDONLY) strcpy (p_stat, "RDONLY");
    else if (tape_info.status & EXPORTED) strcpy (p_stat, "EXPORTED");
    else if (tape_info.status & DISABLED) strcpy (p_stat, "DISABLED");
    else strcpy (p_stat, "?");
    fprintf (stderr, "Volume %s has status %s\n", vid, p_stat);
    exit (USERR);
  }

  /* check if the volume still contains files */
  flags = CNS_LIST_BEGIN;
  while ((dtp = Cns_listtape (host, vid, flags, &list, 0)) != NULL) {
    flags = CNS_LIST_CONTINUE;
    if (dtp->s_status == 'D') {
      fprintf (stderr,
          "Volume %s still contains disabled segments\n", vid);
    } else {
      fprintf (stderr,
          "Volume %s still contains active files\n", vid);
    }
    exit (USERR);
  }
  if (serrno > 0 && serrno != ENOENT) {
    fprintf (stderr, "reclaim %s: %s\n", vid,
        sstrerror(serrno));
    exit (SYERR);
  }
  if (serrno == 0) {
    (void) Cns_listtape (host, vid, CNS_LIST_END, &list, 0);
  }

  /* reset nbfiles, estimated_free_space and status for this volume */
  if (vmgr_reclaim (vid)) {
    fprintf (stderr, "reclaim %s: %s\n", vid,
        (serrno == ENOENT) ? "No such volume" : sstrerror(serrno));
    exit (USERR);
  }
  exit (0);
}
