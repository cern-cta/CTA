/*
 * Copyright (C) 1999-2002 by CERN
 * All rights reserved
 */

/*      Cns_delsegbycopyno - delete file segment by copyno */

#include <errno.h>
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
Cns_delsegbycopyno(const char *path, struct Cns_fileid *file_uniqueid, int copyno)
{
  /* Variables */
  char  *actual_path;
  char  func[19];
  char  sendbuf[REQBUFSZ];
  char  server[CA_MAXHOSTNAMELEN+1];
  char  *q;
  char  *sbp;
  int   msglen;
  int   c;
  gid_t gid;
  uid_t uid;
  struct Cns_api_thread_info *thip;
  u_signed64 zero = 0;

  strcpy (func, "Cns_delsegbycopyno");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
    Cns_errmsg (func, NS053);
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Check that either or path or fileid is supplied */
  if (!path && !file_uniqueid) {
    serrno = EFAULT;
    return (-1);
  }

  /* Check that the path is not too long */
  if (path && strlen (path) > CA_MAXPATHLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }

  /* Check that the copy number is positive */
  if (copyno < 0) {
    serrno = EINVAL;
    return (-1);
  }

  /* Determine the server to contact */
  if (file_uniqueid && *file_uniqueid->server)
    strcpy (server, file_uniqueid->server);
  else
    if (Cns_selectsrvr (path, thip->server, server, &actual_path))
      return (-1);

  /* Build the request header */
  sbp = sendbuf;
  marshall_LONG(sbp, CNS_MAGIC7);
  marshall_LONG(sbp, CNS_DELSEGBYCOPYNO);
  q = sbp;                   /* Save the pointer, for field updates */
  msglen = 3 * LONGSIZE;
  marshall_LONG(sbp, msglen);

  /* Build the request body */
  marshall_LONG(sbp, uid);
  marshall_LONG(sbp, gid);
  marshall_HYPER (sbp, thip->cwd);
  if (file_uniqueid && *file_uniqueid->server) {
    marshall_HYPER (sbp, file_uniqueid->fileid);
    marshall_STRING (sbp, "");
  } else {
    marshall_HYPER (sbp, zero);
    marshall_STRING (sbp, actual_path);
  }
  marshall_LONG(sbp, copyno);

  msglen = sbp - sendbuf;
  marshall_LONG(q, msglen);  /* Update the length field */

  /* Send message to name server daemon */
  c = send2nsd(NULL, server, sendbuf, msglen, NULL, 0);
  if (c && serrno == SENAMETOOLONG) {
    serrno = ENAMETOOLONG;
  }
  return (c);
}
