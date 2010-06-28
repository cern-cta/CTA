/*
 * Copyright (C) 2003 by CERN/IT/GD/CT
 * All rights reserved
 */

/* Cns_readlink - read value of symbolic link */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int DLL_DECL
Cns_readlink(const char *path, char *buf, size_t bufsiz)
{
  char *actual_path;
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *p;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strcpy (func, "Cns_readlink");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! path || ! buf) {
    serrno = EFAULT;
    return (-1);
  }

  if (strlen (path) > CA_MAXPATHLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }

  if (Cns_selectsrvr (path, thip->server, server, &actual_path))
    return (-1);

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_READLINK);
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

  c = send2nsd (NULL, server, sendbuf, msglen, buf, bufsiz);
  if (c == 0) {
    if ((p = memchr (buf, '\0', bufsiz)))
      c = p - buf + 1;
    else
      c = bufsiz;
  }
  if (c < 0 && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
  return (c);
}
