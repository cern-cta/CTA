/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Ctape_test1.c,v $ $Revision: 1.6 $ $Date: 2005/01/20 16:32:12 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Ctape_test1 - test program 1: copy one tape to another */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape_api.h"
char *dvrname;

main(argc, argv)
int argc;
char **argv;
{
	char *buffer;
	int bytestobewritten;
	int c;
	int count;
	char dgn1[CA_MAXDGNLEN+1], dgn2[CA_MAXDGNLEN+1];
	struct dgn_rsv dgn_rsv[2];
	char drive1[CA_MAXUNMLEN+1], drive2[CA_MAXUNMLEN+1];
	int eovflag;
	int errflg;
	char errbuf[512];
	char fid[CA_MAXFIDLEN+1];
	int inblksize;
	int ifd;
	int infseq;
	char inrecfm[CA_MAXRECFMLEN+1];
	char ipath[CA_MAXPATHLEN+1];
	static char labelid[2][4] = {"EOF","EOV"};
	int lrecl;
	int n;
	int nblocks;
	int nbytes;
	int ofd;
	char opath[CA_MAXPATHLEN+1];
	int outblksize;
	int outfseq;
	char outrecfm[CA_MAXRECFMLEN+1];
	char *vid1, *vid2;

	if (argc != 3) {
		usage (argv[0]);
		exit (1);
	}
	vid1 = argv[1];
	vid2 = argv[2];

	/* get device group names from TMS */

	if (getdgn (vid1, dgn1))
		exit (2);
	if (getdgn (vid2, dgn2))
		exit (2);

	/* set receiving buffer for error messages */
 
	Ctape_seterrbuf (errbuf, sizeof(errbuf));

	/* reserve resources */

	strcpy (dgn_rsv[0].name, dgn1);
	if (strcmp (dgn1, dgn2) == 0) {
		dgn_rsv[0].num = 2;
		count = 1;
	} else {
		dgn_rsv[0].num = 1;
		strcpy (dgn_rsv[1].name, dgn2);
		dgn_rsv[1].num = 1;
		count = 2;
	}
	if (c = Ctape_reserve (count, dgn_rsv)) {
		fprintf (stderr, "%s", errbuf);
		exit (c);
	}

	/* mount and position the input tape */

	strcpy (ipath, tempnam(NULL, "tp"));
	if (c = Ctape_mount (ipath, vid1, 0, dgn1, NULL, NULL, WRITE_DISABLE,
	    NULL, NULL, 0)) {
		fprintf (stderr, "%s", errbuf);
		exit (c);
	}
	if (c = Ctape_position (ipath, TPPOSIT_FSEQ, 1, 1, 0, 0, 0, CHECK_FILE,
	    NULL, NULL, NULL, 0, 0, 0, 0)) {
		fprintf (stderr, "%s", errbuf);
		exit (c);
	}
	if (c = Ctape_info (ipath, &inblksize, NULL, NULL, NULL, drive1, fid,
	    &infseq, &lrecl, inrecfm)) {
		fprintf (stderr, "%s", errbuf);
		exit (c);
	}

	/* mount and position the output tape */

	strcpy (opath, tempnam(NULL, "tp"));
	if (c = Ctape_mount (opath, vid2, 0, dgn2, NULL, NULL, WRITE_ENABLE,
	    NULL, NULL, 0)) {
		fprintf (stderr, "%s", errbuf);
		exit (c);
	}
	if (c = Ctape_position (opath, TPPOSIT_FSEQ, 1, 1, 0, 0, 0, NEW_FILE,
	    NULL, NULL, NULL, 0, 0, 0, 0)) {
		fprintf (stderr, "%s", errbuf);
		exit (c);
	}
	if (c = Ctape_info (opath, &outblksize, NULL, NULL, NULL, drive1, fid,
	    &outfseq, &lrecl, outrecfm)) {
		fprintf (stderr, "%s", errbuf);
		exit (c);
	}
	printf ("copying file %d from %s mounted on %s to file %d on %s mounted on %s\n",
		infseq, vid1, drive1, outfseq, vid2, drive2);

	/* allocate buffer */

	if ((buffer = malloc (outblksize)) == NULL) {
		fprintf (stderr, "could not allocate memory for I/O buffer\n");
		exit (1);
	}

	/* open input/output tape files */

	if (ifd = open (ipath, O_RDONLY)) {
		fprintf (stderr, "TP042 - %s : open error : %s\n", ipath,
		    strerror(errno));
		exit (c);
	}
	if (ofd = open (opath, O_RDWR)) {
		fprintf (stderr, "TP042 - %s : open error : %s\n", ipath,
		    strerror(errno));
		exit (c);
	}

	/* copy loop */

	bytestobewritten = 0;
	eovflag = 0;
	errflg = 0;
	nblocks = 0;
	nbytes = 0;

	while (1) {
		if ((n = read (ifd, buffer + bytestobewritten, inblksize)) < 0) {
			rpttperror ("Ctape_test1", ifd, ipath, "read");
			errflg++;
			break;
		}
		if (n == 0) break;	/* end of file reached */
		nbytes += n;
		bytestobewritten += n;
		if (bytestobewritten == outblksize) {
			nblocks++;
			if (nblocks == 1 && wrthdrlbl (ofd, opath) < 0) {
				rpttperror ("Ctape_test1", ofd, opath, "write");
				errflg++;
				break;
			}
			if ((n = write (ofd, buffer, bytestobewritten)) <= 0) {
				c = rpttperror ("Ctape_test1", ofd, opath, "write");
				if (c == ENOSPC) {
					eovflag = 1;
					/* could switch to next volume */
				} else {
					errflg++;
					break;
				}
			}
			bytestobewritten = 0;
		}
	}

	/* flush buffer */

	if (! errflg && bytestobewritten) {
		nblocks++;
		if (nblocks == 1 && wrthdrlbl (ofd, opath) < 0) {
			rpttperror ("Ctape_test1", ofd, opath, "write");
			errflg++;
		} else if ((n = write (ofd, buffer, bytestobewritten)) <= 0) {
			rpttperror ("Ctape_test1", ofd, opath, "write");
			errflg++;
		}
	}

	/* write trailer label */

	if (! errflg && nblocks &&
	    wrttrllbl (ofd, opath, labelid[eovflag], nblocks) < 0 ) {
		rpttperror ("Ctape_test1", ofd, opath, "write");
		errflg++;
	}

	if (!errflg)
		printf ("copy of %d bytes was successful\n", nbytes);
	else
		printf ("copy failed\n");

	/* release resources */

	if (c = Ctape_rls (NULL, TPRLS_ALL)) {
		fprintf (stderr, "%s", errbuf);
		exit (c);
	}
	exit (errflg ? 1 : 0);
}

getdgn(vid, dgn)
char *vid;
char *dgn;
{
	int c, j;
	int repsize;
	int reqlen;
	char tmsdgn[CA_MAXDGNLEN+1];
	char tmrepbuf[132];
	char tmsreq[80];

	sprintf (tmsreq, "QVOL %s (GENERIC SHIFT MESSAGE", vid);
	reqlen = strlen (tmsreq);
	repsize = sizeof(tmrepbuf);
	c = sysreq ("TMS", tmsreq, &reqlen, tmrepbuf, &repsize);
	if (c) {
		fprintf (stderr, "%s\n", tmrepbuf);
		return (c);
	}
	strncpy (tmsdgn, tmrepbuf+ 25, 6);
	tmsdgn[6] = '\0';
	for  (j = 0; tmsdgn[j]; j++)
		if (tmsdgn[j] == ' ') break;
	tmsdgn[j] = '\0';
	strcpy (dgn, tmsdgn);
	return (0);
}

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "input_vid target_vid\n");
}
