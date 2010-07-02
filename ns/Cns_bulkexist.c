/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_bulkexist - check the existence of files in bulk mode. Returns list of non existent files */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <netinet/in.h>
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

/* Cns_bulkexist - check existence of files in bulk mode */

int
Cns_bulkexist(const char* server, u_signed64 *fileIds, int *nbFileIds)
{
  char func[16];
  int msglen;
  char *rbp;
  char *repbuf;
  char *sbp;
  char* sendbuf;
  int i, c;
  gid_t gid;
  uid_t uid;
  struct Cns_api_thread_info *thip;

  strcpy (func, "Cns_bulkexist");
  if (Cns_apiinit (&thip))
    return (-1);
  Cns_getid(&uid, &gid);

  /* Build request header */
  msglen = 6 * LONGSIZE + (*nbFileIds) * HYPERSIZE;
  sendbuf = (char*) malloc(msglen);
  sbp = sendbuf;
  marshall_LONG (sbp, CNS_MAGIC2);
  marshall_LONG (sbp, CNS_BULKEXIST);
  marshall_LONG (sbp, msglen);
  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_LONG (sbp, *nbFileIds);

  /* Build request body */
  for (i = 0; i < *nbFileIds; i++) {
    marshall_HYPER (sbp, fileIds[i]);
  }

  repbuf = (char*) malloc(msglen);
  c = send2nsd (NULL, (char*)server, sendbuf, msglen, repbuf, msglen);
  if (c == 0) {
    rbp = repbuf;
    unmarshall_LONG (rbp, *nbFileIds);
    for (i = 0; i < *nbFileIds; i++) {
      unmarshall_HYPER (rbp, fileIds[i]);
    }
  } else {
    *nbFileIds = 0;
  }
  free(sendbuf);
  free(repbuf);
  return (c);
}
