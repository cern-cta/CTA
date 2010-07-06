/*
 * $Id: rfioacct.c,v 1.9 2008/07/31 13:16:55 sponcec3 Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM Olof Barring
 * All rights reserved
 */

#define RFIO_KERNEL 1

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>

#include "rfio.h"
#include "wsacct.h"
#include "rfioacct.h"

void rfioacct(int reqtype,
              uid_t uid,
              gid_t gid,
              int ns,
              int flag1,
              int flag2,
              int status,
              int rc,
              struct rfiostat *infop,
              char *filename1,
              char *filename2)
{
  int acctreclen;
  struct acctrfio64  acct_rfio;
  struct sockaddr_in local_addr;
  struct sockaddr_in remote_addr;
  socklen_t addr_len;
  static int ACCTRFIO_ON = -1;
  char *getconfent();
  char *p = NULL;
  static int jid = -1;

  if ( ACCTRFIO_ON == -1 ) {
    if (((p == NULL) && ((p = getconfent("ACCT", "RFIO", 0)) == NULL)) ||
        (strcmp (p, "YES") && strcmp (p, "yes"))) ACCTRFIO_ON = 0;
    else ACCTRFIO_ON = 1;
  }
  if (jid == -1) jid = getpid();
  if ( ACCTRFIO_ON == 0 ) return;
  if ( ns < 0 ) return;
  memset(&acct_rfio,'\0',sizeof(struct acctrfio64));
  acct_rfio.reqtype = (int) reqtype;
  acct_rfio.uid = (int) uid;
  acct_rfio.gid = (int) gid;
  acct_rfio.jid = (int) jid;
  acct_rfio.accept_socket = ns;
  acct_rfio.flags.anonymous.flag1 = (int)flag1;
  acct_rfio.flags.anonymous.flag2 = (int)flag2;
  if ( infop != NULL ) {
    acct_rfio.nb_read    = (int) infop->readop;
    acct_rfio.nb_write   = (int) infop->writop;
    acct_rfio.nb_ahead   = (int) infop->aheadop;
    acct_rfio.nb_stat    = (int) infop->statop;
    acct_rfio.nb_seek    = (int) infop->seekop;
    acct_rfio.nb_preseek = (int) infop->presop;
    acct_rfio.read_size  = infop->rnbr;
    acct_rfio.write_size = infop->wnbr;
  }

  if (ns >= 0 ) {
    addr_len = sizeof(local_addr);
    getsockname(ns,(struct sockaddr *)&local_addr,&addr_len);
    acct_rfio.local_addr = (int)local_addr.sin_addr.s_addr;
    addr_len = sizeof(remote_addr);
    getpeername(ns,(struct sockaddr *)&remote_addr,&addr_len);
    acct_rfio.remote_addr = (int)remote_addr.sin_addr.s_addr;
  }
  if (filename1 != NULL ) {
    acct_rfio.len1 = min(2*MAXPATH,strlen(filename1));
    strncpy(&acct_rfio.filename[0],filename1,acct_rfio.len1);
  }
  if (filename2 != NULL ) {
    acct_rfio.len2 = min(2*MAXPATH-acct_rfio.len1,(int)strlen(filename2));
    strncpy(&acct_rfio.filename[acct_rfio.len1],filename2,acct_rfio.len2);
  }
  acct_rfio.status = status;
  acct_rfio.rc = rc;
  acctreclen = ((char *)acct_rfio.filename - (char *) &acct_rfio) + strlen(acct_rfio.filename) + 1;
  wsacct(ACCTRFIO64, (void*)&acct_rfio, acctreclen);
  return;
}
