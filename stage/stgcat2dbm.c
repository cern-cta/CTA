/*
 * $Id: stgcat2dbm.c,v 1.4 2000/03/23 01:41:50 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <fcntl.h>
#include <ndbm.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "stage.h"
extern char *sys_errlist[];

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stgcat2dbm.c,v $ $Revision: 1.4 $ $Date: 2000/03/23 01:41:50 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

main()
{
	datum content;
	int c, i;
	DBM *fdb;
	datum key;
	int nbcat_ent;
	int scfd;
	struct stat st;
	struct stgcat_entry stgreq;

	/* read stage catalog */

	if ((scfd = open (STGCAT, O_RDONLY)) < 0) {
		fprintf (stderr, STG02, STGCAT, "open", sys_errlist[errno]);
		exit (USERR);
	}
	if (fstat (scfd, &st) < 0) {
		fprintf (stderr, STG02, STGCAT, "fstat", sys_errlist[errno]);
		exit (USERR);
	}
	if (st.st_size == 0) {
		fprintf (stderr, "%s is empty\n", STGCAT);
		exit (0);
	}
	nbcat_ent = (st.st_size / sizeof(struct stgcat_entry));
	if (st.st_size != nbcat_ent * sizeof(struct stgcat_entry)) {
		fprintf (stderr, STG48, STGCAT);
		exit (SYERR);
	}
	if ((fdb = dbm_open (STGCAT, O_RDWR | O_CREAT, 0644)) == NULL) {
		fprintf (stderr, STG02, STGCAT, "dbm_open", sys_errlist[errno]);
		exit (USERR);
	}
	for (i = 0; i < nbcat_ent; i++) {
		if (read (scfd, (char *) &stgreq, sizeof(struct stgcat_entry)) < 0) {
			fprintf (stderr, STG02, STGCAT, "read", sys_errlist[errno]);
			exit (USERR);
		}
		key.dptr = (char *) &stgreq.reqid;
		key.dsize = sizeof(int);
		content.dptr = (char *) &stgreq;
		content.dsize = sizeof(struct stgcat_entry);
		if ((c = dbm_store (fdb, key, content, DBM_INSERT)) < 0) {
			fprintf (stderr, STG02, STGCAT, "dbm_store", "");
			exit (SYERR);
		}
		if (c) {
			fprintf (stderr, "duplicate reqid %d\n", stgreq.reqid);
			exit (USERR);
		}
		
	}
	(void) dbm_close (fdb);
	exit (0);
}
