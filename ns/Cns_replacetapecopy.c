/*
 * Copyright (C) 2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_replaceseg - replace file segment (used by repack) */

#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int
Cns_replaceormovetapecopy(struct Cns_fileid *file_uniqueid,
                          const char* originalvid, int originalcopynb,
                          struct Cns_segattrs *newsegattrs,
                          time_t last_mod_time)
{
  int c;
  int msglen;
  char func[20];

  gid_t gid;
  uid_t uid;

  char *q;
  char *sbp;
  char *actual_path;
  char server[CA_MAXHOSTNAMELEN+1];
  char sendbuf[REQBUFSZ];
  struct Cns_api_thread_info *thip;

  strncpy (func, "Cns_replaceormovetapecopy", 20);
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  if (! newsegattrs || !originalvid || !file_uniqueid ) {
    serrno = EFAULT;
    return (-1);
  }

  /* set the nameserver */
  if (file_uniqueid && *file_uniqueid->server) {
    strcpy (server, file_uniqueid->server);
  } else {
    if (Cns_selectsrvr (NULL, thip->server, server, &actual_path))
      return (-1);
  }

  /* Build request header */
  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC5);
  marshall_LONG (sbp, CNS_REPLACEORMOVETAPECOPY);
  q = sbp;        /* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_HYPER (sbp, file_uniqueid->fileid);
  marshall_TIME_T (sbp, last_mod_time);
  marshall_STRING (sbp, originalvid);
  marshall_LONG (sbp, originalcopynb);

  /* we can only have one segment as multisegmented files have been dropped */
  marshall_WORD (sbp, (newsegattrs)->copyno);
  marshall_WORD (sbp, (newsegattrs)->fsec);
  marshall_HYPER (sbp, (newsegattrs)->segsize);
  marshall_LONG (sbp, (newsegattrs)->compression);
  marshall_BYTE (sbp, (newsegattrs)->s_status);
  marshall_STRING (sbp, (newsegattrs)->vid);
  marshall_WORD (sbp, (newsegattrs)->side);
  marshall_LONG (sbp, (newsegattrs)->fseq);
  marshall_OPAQUE (sbp, (newsegattrs)->blockid, 4);
  marshall_STRING (sbp, (newsegattrs)->checksum_name);
  marshall_LONG (sbp, (newsegattrs)->checksum);

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen); /* update length field */

  c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
  return (c);
}
