/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */


#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "osdep.h"
#include "wsacct.h"

int tapeacct(int subtype,
             uid_t uid,
             gid_t gid,
             int jid,
             char *dgn,
             char *drive,
             char *vid,
             int fseq,
             int reason)
{
	struct accttape accttape;
	char *getconfent();
	char *p;

	if ((p = getconfent("ACCT", "TAPE", 0)) != NULL &&
	    (strcmp (p, "YES") && strcmp (p, "yes"))) return (0);
	memset ((char *) &accttape, 0, sizeof(struct accttape));
	accttape.subtype = subtype;
	accttape.uid = uid;
	accttape.gid = gid;
	accttape.jid = jid;
	strcpy (accttape.dgn, dgn);
	strcpy (accttape.drive, drive);
	strcpy (accttape.vid, vid);
	accttape.fseq = fseq;
	accttape.reason = reason;

	wsacct (ACCTTAPE, &accttape, sizeof(struct accttape));
	return (0);
}
