/*
 * Copyright (C) 2000-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "serrno.h"
#include "u64subr.h"
#include "vmgr.h"
#include "vmgr_api.h"

int main(int argc, char **argv) {
  int        rc   = 0;
  const char *vid = NULL;
  const int  side = 0;
  char       dgn[CA_MAXDGNLEN+1];
  struct     vmgr_tape_info vmgrTapeInfo;

  if(argc != 2) {
    fprintf(stderr, "Error: Incorrect number of parameters\n");
    fprintf(stderr, "Usage: vmgrquerytape_trunk_r21843 VID\n");
    exit(-1);
  }

  vid = argv[1];
  if(strlen(vid) > CA_MAXVIDLEN) {
    fprintf(stderr, "Error: VID is greater than %d characters\n",
      CA_MAXVIDLEN);
    exit(-1);
  }

  memset(dgn, 0, sizeof(dgn));
  memset(&vmgrTapeInfo, 0, sizeof(vmgrTapeInfo));
  if(vmgr_querytape(vid, side, &vmgrTapeInfo, dgn)) {
    fprintf(stderr, "vmgr_querytape failed: vid=\"%s\": %s\n", vid,
      sstrerror(serrno));
    exit(-1);
  }

  printf("vmgr_querytape succeeded: vid=\"%s\" dgn=\"%s\"\n", vid, dgn);

  return 0;
}
