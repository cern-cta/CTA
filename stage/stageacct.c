/*
 * $Id: stageacct.c,v 1.15 2002/11/22 08:54:01 jdurand Exp $
 */

/*
 * Copyright (C) 1995-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageacct.c,v $ $Revision: 1.15 $ $Date: 2002/11/22 08:54:01 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "stage.h"
#include "sacct.h"

#if defined(sun)
/* On Sun old style declaration char promoted to int */
void stageacct _PROTO(());
#else
void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
#endif
extern int wsacct _PROTO((int, char *, int));

#ifdef hpux
void stageacct(int subtype, uid_t uid, gid_t gid, char *clienthost, int reqid, int req_type, int retryn, int exitcode, struct stgcat_entry *stcp, char *tapesrvr, char forced_t_or_d)
#else
void
stageacct(subtype, uid, gid, clienthost, reqid, req_type, retryn, exitcode, stcp, tapesrvr, forced_t_or_d)
	int subtype;
	uid_t uid;
	gid_t gid;
	char *clienthost;
	int reqid;
	int req_type;
	int retryn;
	int exitcode;
	struct stgcat_entry *stcp;
	char *tapesrvr;
	char forced_t_or_d;
#endif
{
  int acctreclen = 0;
  struct acctstage64 acctstage;
  char *getconfent();
  char *p;
  
  if ((p = getconfent("ACCT", "STAGE", 0)) == NULL ||
      (strcmp (p, "YES") && strcmp (p, "yes"))) return;
  memset ((char *) &acctstage, 0, sizeof(struct acctstage64));
  acctstage.subtype = subtype;
  acctstage.uid = uid;
  acctstage.gid = gid;
  acctstage.reqid = reqid;
  acctstage.req_type = req_type;
  acctstage.retryn = retryn;
  acctstage.exitcode = exitcode;
  if (clienthost != NULL) {
    strcpy (acctstage.u2.clienthost, clienthost);
  }
  if (stcp) {
    strcpy (acctstage.u2.s.poolname, stcp->poolname);
    acctstage.u2.s.t_or_d = stcp->t_or_d;
    acctstage.u2.s.actual_size = stcp->actual_size;
    acctstage.u2.s.c_time = stcp->c_time;
    acctstage.u2.s.nbaccesses = stcp->nbaccesses;
    switch (stcp->t_or_d) {
    case 't':
      acctstage.u2.s.u1.t.side = stcp->u1.t.side;
      strcpy (acctstage.u2.s.u1.t.dgn, stcp->u1.t.dgn);
      strcpy (acctstage.u2.s.u1.t.fseq, stcp->u1.t.fseq);
      strcpy (acctstage.u2.s.u1.t.vid, stcp->u1.t.vid[0]);
      strcpy (acctstage.u2.s.u1.t.tapesrvr, tapesrvr);
      acctreclen = ((char *) acctstage.u2.s.u1.t.tapesrvr
					- (char *) &acctstage) + strlen (tapesrvr) + 1;
      break;
    case 'd':
    case 'a':
      strcpy (acctstage.u2.s.u1.d.xfile, stcp->u1.d.xfile);
      acctreclen = ((char *) acctstage.u2.s.u1.d.xfile
					- (char *) &acctstage) + strlen (stcp->u1.d.xfile) + 1;
      break;
    case 'm':
      strcpy (acctstage.u2.s.u1.m.xfile, stcp->u1.m.xfile);
      acctreclen = ((char *) acctstage.u2.s.u1.m.xfile
					- (char *) &acctstage) + strlen (stcp->u1.m.xfile) + 1;
      break;
    case 'h':
      strcpy (acctstage.u2.s.u1.h.xfile, stcp->u1.h.xfile);
      acctstage.u2.s.u1.h.fileid = stcp->u1.h.fileid;
      acctreclen = ((char *) &(acctstage.u2.s.u1.h.fileid)
					- (char *) &acctstage) + sizeof(stcp->u1.h.fileid);
      break;
    }
  } else {
    if (forced_t_or_d) acctstage.u2.s.t_or_d = forced_t_or_d;
    /* If clienthost == NULL, then acctstage.u2.clienthost == '\0' */
    acctreclen = ((char *) acctstage.u2.clienthost
                  - (char *) &acctstage) + strlen (acctstage.u2.clienthost) + 1;
  }
  if (acctreclen) wsacct (ACCTSTAGE64, (char *) &acctstage, acctreclen);
}
