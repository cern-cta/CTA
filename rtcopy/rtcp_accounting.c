/*
 * Copyright (C) 1995-2000 by CERN IT-PDP/DM Olof Barring
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcp_accounting.c,v $ $Revision: 1.1 $ $Date: 2000/02/11 11:39:26 $ CERN IT-PDP/DM Olof Barring"
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include <sacct.h>

rtcp_wacct(int   subtype, 
           uid_t uid, 
           gid_t gid, 
           int   jid, 
           int   stgreqid, 
           char  reqtype, 
           char  ifce, 
           char  vid, 
           int   size, 
           int   retryn, 
           int   exitcode, 
           char  *clienthost, 
           char  *dsksrvr,
           char  *errmsgtxt)
{
    int acctreclen;
    struct acctrtcp acctrtcp;
    char *getconfent();
    char *p;

    if ((p = getconfent("ACCT", "RTCOPY", 0)) == NULL ||
        (strcmp (p, "YES") && strcmp (p, "yes"))) return;
    memset ((char *) &acctrtcp, 0, sizeof(struct acctrtcp));
    acctrtcp.subtype = subtype;
    acctrtcp.uid = uid;
    acctrtcp.gid = gid;
    acctrtcp.jid = jid;
    acctrtcp.stgreqid = stgreqid;
    acctrtcp.reqtype = reqtype;
    strcpy (acctrtcp.ifce, ifce);
    strcpy (acctrtcp.vid, vid);
    acctrtcp.size = size;
    acctrtcp.retryn = retryn;
    acctrtcp.exitcode = exitcode;
    strcpy (acctrtcp.clienthost, clienthost);
    strcpy (acctrtcp.dsksrvr, dsksrvr);
    strcpy (acctrtcp.errmsgtxt, errmsgtxt);
    acctreclen = ((char *) acctrtcp.errmsgtxt - (char *) &acctrtcp) +
                 strlen(errmsgtxt)+ 1;

    wsacct (ACCTRTCOPY, &acctrtcp, acctreclen);
}
