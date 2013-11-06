/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_closedir - free the Cns_DIR structure */

#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <netinet/in.h>
#include "Cns_api.h"
#include "Cns.h"
#include "marshall.h"
#include "serrno.h"

int
Cns_closedir(Cns_DIR *dirp)
{
  int msglen;
  char *sbp;
  char sendbuf[REQBUFSZ];

  if (! dirp) {
    serrno = EFAULT;
    return (-1);
  }

  /* tell nsd to free the thread */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_CLOSEDIR);
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);
  (void) send2nsd (&dirp->dd_fd, NULL, sendbuf, msglen, NULL, 0);
  free (dirp);
  return (0);
}
