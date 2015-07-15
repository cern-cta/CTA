/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_getcomment - get the comment associated with a file/directory */

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
Cns_getcomment(const char *path, char *comment)
{
  char *actual_path;
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *q;
  char *rbp;
  char repbuf[CA_MAXCOMMENTLEN+1];
  char *sbp;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strncpy (func, "Cns_getcomment", 16);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! path || ! comment) {
    serrno = EFAULT;
    return (-1);
  }

  if (Cns_selectsrvr (path, thip->server, server, &actual_path))
    return (-1);

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_GETCOMMENT);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, thip->cwd);
  marshall_STRING (sbp, actual_path);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {
    rbp = repbuf;
    unmarshall_STRING (rbp, comment);
  }
  if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
  return (c);
}
