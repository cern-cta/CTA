/*
 * $Id: stageacct.c,v 1.3 1999/07/21 20:09:06 jdurand Exp $
 *
 * $Log: stageacct.c,v $
 * Revision 1.3  1999/07/21 20:09:06  jdurand
 * Initialize all variable pointers to NULL
 *
 * Revision 1.2  1999/07/20 17:29:20  jdurand
 * Added Id and Log CVS's directives
 *
 */

/*
 * Copyright (C) 1995 by CERN/CN/PDP/DH
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)stageacct.c	1.2 07/31/95 CERN CN-PDP/DH Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include "stage.h"
#include "../h/sacct.h"

stageacct(subtype, uid, gid, clienthost, reqid, req_type, retryn, exitcode, stcp, tapesrvr)
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
{
	int acctreclen;
	struct acctstage acctstage;
	char *getconfent();
	char *p = NULL;

	if ((p = getconfent("ACCT", "STAGE", 0)) == NULL ||
	    (strcmp (p, "YES") && strcmp (p, "yes"))) return;
	memset ((char *) &acctstage, 0, sizeof(struct acctstage));
	acctstage.subtype = subtype;
	acctstage.uid = uid;
	acctstage.gid = gid;
	acctstage.reqid = reqid;
	acctstage.req_type = req_type;
	acctstage.retryn = retryn;
	acctstage.exitcode = exitcode;
	if (stcp) {
		strcpy (acctstage.u2.s.poolname, stcp->poolname);
		acctstage.u2.s.t_or_d = stcp->t_or_d;
		acctstage.u2.s.actual_size = stcp->actual_size;
		acctstage.u2.s.nbaccesses = stcp->nbaccesses;
		if (stcp->t_or_d == 't') {
			strcpy (acctstage.u2.s.u1.t.dgn, stcp->u1.t.dgn);
			strcpy (acctstage.u2.s.u1.t.fseq, stcp->u1.t.fseq);
			strcpy (acctstage.u2.s.u1.t.vid, stcp->u1.t.vid[0]);
			strcpy (acctstage.u2.s.u1.t.tapesrvr, tapesrvr);
			acctreclen = ((char *) acctstage.u2.s.u1.t.tapesrvr
			    - (char *) &acctstage) + strlen (tapesrvr) + 1;
		} else {
			strcpy (acctstage.u2.s.u1.d.xfile, stcp->u1.d.xfile);
			acctreclen = ((char *) acctstage.u2.s.u1.d.xfile
			    - (char *) &acctstage) + strlen (stcp->u1.d.xfile) + 1;
		}
	} else {
		strcpy (acctstage.u2.clienthost, clienthost);
		acctreclen = ((char *) acctstage.u2.clienthost
		    - (char *) &acctstage) + strlen (clienthost) + 1;
	}

	wsacct (ACCTSTAGE, &acctstage, acctreclen);
}
