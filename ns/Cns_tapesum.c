/*
 * Copyright (C) 2000-2004 by CERN
 * All rights reserved
 */

/* Cns_tapesum - returns the total size and number of files on a volume */

/* headers */
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int DLL_DECL
Cns_tapesum(const char *vid, u_signed64 *count, u_signed64 *size, u_signed64 *maxfileid, int filter)
{
  /* variables */
  char  func[16];
  char  sendbuf[REQBUFSZ];
  char  repbuf[REPBUFSZ];
  char  *q;
  char  *sbp;
  char  *rbp;
  int   msglen;
  int   c;
  gid_t gid;
  uid_t uid;
  struct Cns_api_thread_info *thip;

  strcpy (func, "Cns_tapesum");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  *count = 0;
  *size  = 0;

#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
    Cns_errmsg(func, NS053);
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Check that VID exists and is not too long */
  if (!vid) {
    serrno = EFAULT;
    return (-1);
  }
  if (strlen(vid) > CA_MAXVIDLEN) {
    serrno = EINVAL;
    return (-1);
  }
  if ((filter < 0) || (filter > 3)) {
    serrno = EINVAL;
    return (-1);
  }

  /* Build the request header */
  sbp = sendbuf;
  marshall_LONG(sbp, CNS_MAGIC5);
  marshall_LONG(sbp, CNS_TAPESUM);
  q = sbp;                   /* Save the pointer, for field updates */
  msglen = 3 * LONGSIZE;
  marshall_LONG(sbp, msglen);

  /* Build the request body */
  marshall_LONG(sbp, uid);
  marshall_LONG(sbp, gid);
  marshall_STRING(sbp, vid);
  marshall_LONG(sbp, filter);

  msglen = sbp - sendbuf;
  marshall_LONG(q, msglen);  /* Update the length field */

  /* Send message to name server daemon */
  c = send2nsd(NULL, NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
  if (c == 0) {
    rbp = repbuf;
    unmarshall_HYPER (rbp, *count);
    unmarshall_HYPER (rbp, *size);
    unmarshall_HYPER (rbp, *maxfileid);
  }
  if (c && serrno == SENAMETOOLONG) {
    serrno = ENAMETOOLONG;
  }
  return (c);
}
