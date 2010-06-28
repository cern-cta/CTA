/*
 * Copyright (C) 2004 by CERN/IT/GD/CT
 * All rights reserved
 */

/* Cns_listlinks - list all link entries for a given file */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

struct Cns_linkinfo *
Cns_listlinks(const char *path, const char *guid, int flags, Cns_list *listp)
{
  char *actual_path;
  int bol = 0;
  int c;
  char func[16];
  gid_t gid;
  int listentsz = sizeof(struct Cns_linkinfo);
  struct Cns_linkinfo *lp;
  int msglen;
  int nbentries;
  char *q;
  char *rbp;
  char repbuf[LISTBUFSZ+4];
  char *sbp;
  char sendbuf[REQBUFSZ];
  char server[CA_MAXHOSTNAMELEN+1];
  struct Cns_api_thread_info *thip;
  uid_t uid;

  strcpy (func, "Cns_listlinks");
  if (Cns_apiinit (&thip))
    return (NULL);
  Cns_getid(&uid, &gid);

  if ((! path && ! guid) || ! listp) {
    serrno = EFAULT;
    return (NULL);
  }

  if ((path && strlen (path) > CA_MAXPATHLEN) ||
      (guid && strlen (guid) > CA_MAXGUIDLEN)) {
    serrno = ENAMETOOLONG;
    return (NULL);
  }

  if (path && Cns_selectsrvr (path, thip->server, server, &actual_path))
    return (NULL);

  if (flags == CNS_LIST_BEGIN) {
    memset (listp, 0, sizeof(Cns_list));
    listp->fd = -1;
    if ((listp->buf = malloc (LISTBUFSZ)) == NULL) {
      serrno = ENOMEM;
      return (NULL);
    }
    bol = 1;
  }
  if (listp->len == 0 && listp->eol /* all entries have been listed */
      && flags != CNS_LIST_END)
    return (NULL);

  if (listp->buf == NULL)
    return (NULL);   /* buffer has already been freed */
  if (listp->len == 0 /* no data in the cache */
      || flags == CNS_LIST_END) {

    /* Build request header */

    sbp = sendbuf;
    marshall_LONG (sbp, CNS_MAGIC);
    if (flags == CNS_LIST_END) {
      marshall_LONG (sbp, CNS_ENDLIST);
    } else {
      marshall_LONG (sbp, CNS_LISTLINKS);
    }
    q = sbp;        /* save pointer. The next field will be updated */
    msglen = 3 * LONGSIZE;
    marshall_LONG (sbp, msglen);

    /* Build request body */

    marshall_LONG (sbp, uid);
    marshall_LONG (sbp, gid);
    marshall_WORD (sbp, listentsz);
    marshall_HYPER (sbp, thip->cwd);
    if (path) {
      marshall_STRING (sbp, actual_path);
    } else {
      marshall_STRING (sbp, "");
    }
    if (guid) {
      marshall_STRING (sbp, guid);
    } else {
      marshall_STRING (sbp, "");
    }
    marshall_WORD (sbp, bol);

    msglen = sbp - sendbuf;
    marshall_LONG (q, msglen); /* update length field */

    c = send2nsd (&listp->fd, path ? server : NULL, sendbuf,
                  msglen, repbuf, sizeof(repbuf));
    if (c < 0 || flags == CNS_LIST_END) {
      if (listp->buf)
        free (listp->buf);
      listp->buf = NULL;
      return (NULL);
    }
    rbp = repbuf;
    unmarshall_WORD (rbp, nbentries);
    if (nbentries == 0)
      return (NULL);  /* end of list */

    /* unmarshall reply into Cns_linkinfo structures */

    lp = (struct Cns_linkinfo *) listp->buf;
    while (nbentries--) {
      unmarshall_STRING (rbp, lp->path);
      lp++;
    }
    unmarshall_WORD (rbp, listp->eol);
    listp->len = (char *) lp - listp->buf;
  }
  lp = (struct Cns_linkinfo *) (listp->buf + listp->offset);
  listp->offset += sizeof(struct Cns_linkinfo);
  if (listp->offset >= listp->len) { /* must refill next time */
    listp->offset = 0;
    listp->len = 0;
  }
  return (lp);
}
