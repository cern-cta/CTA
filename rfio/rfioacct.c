/*
 * Copyright (C) 1998 by CERN/IT/PDP/DM Olof Barring
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)rfioacct.c	1.2 01/14/99 CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

#define RFIO_KERNEL 1

#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <pwd.h>
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif

#include "rfio.h"
#include "sacct.h"

void rfioacct(reqtype,uid,gid,ns,flag1,flag2,status,rc,infop,filename1,filename2)
int reqtype;
uid_t uid;
gid_t gid;
int ns;
int flag1;
int flag2;
int status;
int rc;
struct rfiostat *infop;
char *filename1,*filename2;
{
  int acctreclen;
  struct acctrfio acct_rfio;
  struct sockaddr_in local_addr;
  struct sockaddr_in remote_addr;
  int addr_len;
  static int ACCTRFIO_ON = -1;
#if defined(HPSS)
  int jid = -1;
#else /* HPSS */
  char *getconfent();
  char *p = NULL;
  static int jid = -1;
#endif /* HPSS */

#if defined(HPSS)
  if ( ACCTRFIO_ON == 0 ) return;
  jid = rhpss_get_jid(ns);
  if ( jid < 0 ) {
    ACCTRFIO_ON = 0;
    return;
  }
#else
  if ( ACCTRFIO_ON == -1 ) {
    if (p == NULL && (p = getconfent("ACCT", "RFIO", 0)) == NULL ||
	(strcmp (p, "YES") && strcmp (p, "yes"))) ACCTRFIO_ON = 0;
    else ACCTRFIO_ON = 1;
  }
  if (jid == -1) jid = getpid();
#endif
  if ( ACCTRFIO_ON == 0 ) return;
  if ( ns < 0 ) return;
  memset(&acct_rfio,'\0',sizeof(struct acctrfio));
  acct_rfio.reqtype = (int) reqtype;
  acct_rfio.uid = (int) uid;
  acct_rfio.gid = (int) gid;
  acct_rfio.jid = (int) jid;
  acct_rfio.accept_socket = ns;
  acct_rfio.flags.anonymous.flag1 = (int)flag1;
  acct_rfio.flags.anonymous.flag2 = (int)flag2;
  if ( infop != NULL ) {
    acct_rfio.nb_read = (int) infop->readop;
    acct_rfio.nb_write = (int) infop->writop;
    acct_rfio.nb_ahead = (int) infop->aheadop;
    acct_rfio.nb_stat = (int) infop->statop;
    acct_rfio.nb_seek = (int) infop->seekop;
    acct_rfio.nb_preseek = (int) infop->presop;
    acct_rfio.read_size = (int) infop->rnbr;
    acct_rfio.write_size = (int) infop->wnbr;
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
    acct_rfio.len2 = min(2*MAXPATH-acct_rfio.len1,strlen(filename2));
    strncpy(&acct_rfio.filename[acct_rfio.len1],filename2,acct_rfio.len2);
  }
  acct_rfio.status = status;
  acct_rfio.rc = rc;
  acctreclen = ((char *)acct_rfio.filename - (char *) &acct_rfio) + strlen(acct_rfio.filename) + 1;
  wsacct(ACCTRFIO,&acct_rfio,acctreclen);
#if defined(HPSS)
  /*
   * release accounting mutex
   */
  /*  rhpss_end_accounting(ns); */
#endif /* HPSS */
  return;
}
