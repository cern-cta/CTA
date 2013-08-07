/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_mkdir - create a directory entry */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int
Cns_mkdirg(const char *path, const char *guid, mode_t mode)
{
  char *actual_path;
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strncpy (func, "Cns_mkdir", 16);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! path) {
    serrno = EFAULT;
    return (-1);
  }

  if (strlen (path) > CA_MAXPATHLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }
  if (guid && strlen (guid) > CA_MAXGUIDLEN) {
    serrno = EINVAL;
    return (-1);
  }

  // Check that only expected bits are set in the mode.
  // Allowed bits are the ones to set permissions to directories
  if ((mode & S_PERMDIR) != mode) {
    serrno = EINVAL;
    return (-1);
  }

  if (Cns_selectsrvr (path, thip->server, server, &actual_path))
    return (-1);

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, guid ? CNS_MAGIC2 : CNS_MAGIC);
  marshall_LONG (sbp, CNS_MKDIR);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_WORD (sbp, thip->mask);
  marshall_HYPER (sbp, thip->cwd);
  marshall_STRING (sbp, actual_path);
  marshall_LONG (sbp, mode);
  if (guid)
    marshall_STRING (sbp, guid);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
  if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
  return (c);
}

int
Cns_mkdir(const char *path, mode_t mode)
{
  return (Cns_mkdirg (path, NULL, mode));
}
