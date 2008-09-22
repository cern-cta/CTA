/*
 * Copyright (C) 2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

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

/*      Cns_updateseg_checksum - Updates the segment checksum */

int DLL_DECL
Cns_updateseg_checksum(char *server, u_signed64 fileid, struct Cns_segattrs *oldsegattrs, struct Cns_segattrs *newsegattrs)
{
  int c;
  char func[16];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strcpy (func, "Cns_replaceseg");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  #
#if defined(_WIN32)
    if (uid < 0 || gid < 0) {
      Cns_errmsg (func, NS053);
      serrno = SENOMAPFND;
      return (-1);
    }
#endif

  if (! oldsegattrs || ! newsegattrs) {
    serrno = EFAULT;
    return (-1);
  }

  /* Check that the members (copyno, fsec)
     of oldsegattrs and newsegattrs are identical */

  if (oldsegattrs->copyno != newsegattrs->copyno ||
      oldsegattrs->fsec != newsegattrs->fsec) {
    serrno = EINVAL;
    return (-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC4);
  marshall_LONG (sbp, CNS_UPDATESEG_CHECKSUM);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, fileid);
  marshall_WORD (sbp, oldsegattrs->copyno);
  marshall_WORD (sbp, oldsegattrs->fsec);

  marshall_STRING (sbp, oldsegattrs->vid);
  marshall_WORD (sbp, oldsegattrs->side);
  marshall_LONG (sbp, oldsegattrs->fseq);

  marshall_STRING (sbp, newsegattrs->checksum_name);
  marshall_LONG (sbp, newsegattrs->checksum);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
  return (c);
}

/*      Cns_updatefile_checksum - Updates the file checksum */

int DLL_DECL
Cns_updatefile_checksum(const char *path, const char *csumtype, const char *csumvalue)
{
  char *actual_path;
  int c;
  char func[24];
  gid_t gid;
  int msglen;
  char *q;
  char *sbp;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strcpy (func, "Cns_updatefile_checksum");
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

  if (! path ) {
    serrno = EFAULT;
    return (-1);
  }

  if (path && strlen (path) > CA_MAXPATHLEN) {
    serrno = ENAMETOOLONG;
    return (-1);
  }

  if ((csumtype && strlen (csumtype) > 2) || (csumvalue && strlen (csumvalue) > 32)) {
    serrno = EINVAL;
    return (-1);
  }

  if (Cns_selectsrvr (path, thip->server, server, &actual_path))
    return (-1);

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC);
  marshall_LONG (sbp, CNS_UPDATEFILE_CHECKSUM);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, thip->cwd);
  marshall_STRING (sbp, actual_path);

  if (csumtype) {
    marshall_STRING (sbp, csumtype);
  } else {
    marshall_STRING (sbp, "");
  }
  if (csumvalue) {
    marshall_STRING (sbp, csumvalue);
  } else {
    marshall_STRING (sbp, "");
  }

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
  if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
  return (c);
}
