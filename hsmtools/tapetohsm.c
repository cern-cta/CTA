/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tapetohsm.c,v $ $Revision: 1.1 $ $Date: 2001/03/28 05:23:38 $ CERN/IT/PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	tapetohsm - catalog in the CASTOR Name Server files from a non HSM tape */
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "Cns_api.h"
#include "serrno.h"
#include "u64subr.h"
#include "vmgr_api.h"

u_signed64 BytesWritten;
char *hsm_path;
int iflag;
int maxfseq;
struct Cns_segattrs segattrs;
struct vmgr_tape_info tape;
int verbose;

main(argc, argv)
int argc;
char **argv;
{
	int c;
	int errflg = 0;
	char tmpbuf[21];
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	segattrs.fsec = 1;
	segattrs.s_status = '-';

	if (c = chkopt (argc, argv, &segattrs, 1)) {
		usage (argv[0]);
		goto prt_final_status;
	}
	if (*tape.vid == '\0' || *tape.model == '\0' || *tape.dgn == '\0') {
		c = USERR;
		usage (argv[0]);
		goto prt_final_status;
	}

#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, "WSAStartup unsuccessful\n");
		c = SYERR;
		goto prt_final_status;
	}
#endif
#if TMS
	if (tmscheck (tape.vid, tape.vsn, tape.dgn, tape.density, tape.lbltype)) {
		c = USERR;
		goto prt_final_status;
	}
#endif

	/* enter the tape in the Volume Manager */

	if (verbose)
		printf ("vmgr_entertape (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %x)\n",
		    tape.vid, tape.vsn, tape.dgn, tape.density,
		    tape.lbltype, tape.model, tape.media_letter, "", "",
		    tape.poolname, TAPE_RDONLY);
	if (vmgr_entertape (tape.vid, tape.vsn, tape.dgn, tape.density,
	    tape.lbltype, tape.model, tape.media_letter, NULL, NULL,
	    tape.poolname, TAPE_RDONLY) < 0) {
		fprintf (stderr, "vmgrentertape %s: %s\n", tape.vid, sstrerror(serrno));
		c = USERR;
		goto prt_final_status;
	}

	if (! iflag)		/* all parameters on command line */
		c = procfile();
	else
		c = procinpdir (stdin);
	if (c)
		goto prt_final_status;

	if (verbose)
		printf ("vmgr_updatetape (%s, %s, 0, %d, 0)\n",
		    tape.vid, u64tostr (BytesWritten, tmpbuf, 0), maxfseq);
	if (vmgr_updatetape (tape.vid, BytesWritten, 0, maxfseq, 0) < 0) {
		fprintf (stderr, "%s: %s\n", tape.vid, sstrerror(serrno));
		errflg++;
	}
	c = errflg ? USERR : 0;
prt_final_status:
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (c);
}

chkopt(nargs, argv, cmdline)
int nargs;
char **argv;
int cmdline;
{
	int c;
	char *dp;
	int errflg = 0;
	static struct Coptions longopts[] = {
		{"media_letter", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"ml", REQUIRED_ARGUMENT, 0, OPT_MEDIA_LETTER},
		{"model", REQUIRED_ARGUMENT, 0, OPT_MODEL},
		{"pool", REQUIRED_ARGUMENT, 0, 'P'},
		{"verbose", NO_ARGUMENT, &verbose, 1},
		{0, 0, 0, 0}
	};

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (nargs, argv, "d:g:il:M:P:q:s:V:v:", longopts, NULL)) != EOF) {
		switch (c) {
		case OPT_MEDIA_LETTER:
			if (strlen (Coptarg) > CA_MAXMLLEN) {
				fprintf (stderr, "%s: %s\n", Coptarg,
				    "media letter too long");
				errflg++;
			} else
				strcpy (tape.media_letter, Coptarg);
			break;
		case OPT_MODEL:
			if (strlen (Coptarg) > CA_MAXMODELLEN) {
				fprintf (stderr, "%s: %s\n", Coptarg,
				    "model name too long");
				errflg++;
			} else
				strcpy (tape.model, Coptarg);
			break;
		case 'd':
			if (strlen (Coptarg) > CA_MAXDENLEN) {
				fprintf (stderr,
				    "invalid density %s\n", Coptarg);
				errflg++;
			} else
				strcpy (tape.density, Coptarg);
			break;
		case 'g':
			if (strlen (Coptarg) > CA_MAXDGNLEN) {
				fprintf (stderr,
				    "invalid dgn %s\n", Coptarg);
				errflg++;
			} else
				strcpy (tape.dgn, Coptarg);
			break;
		case 'i':
			if (! cmdline) {
				fprintf (stderr,
				    "option -i is only valid on the command line\n");
				errflg++;
			} else
				iflag++;
			break;
		case 'l':
			if (strlen (Coptarg) > CA_MAXDENLEN) {
				fprintf (stderr,
				    "invalid lbltype %s\n", Coptarg);
				errflg++;
			} else
				strcpy (tape.lbltype, Coptarg);
			break;
		case 'M':
			if (strlen (Coptarg) > CA_MAXPATHLEN) {
				fprintf (stderr, "%s: %s\n", Coptarg,
				    sstrerror(SENAMETOOLONG));
				errflg++;
			} else
				hsm_path = Coptarg;
			break;
		case 'P':
			if (strlen (Coptarg) > CA_MAXPOOLNAMELEN) {
				fprintf (stderr, "%s: %s\n", Coptarg,
				    "pool name too long");
				errflg++;
			} else
				strcpy (tape.poolname, Coptarg);
			break;
		case 'q':
			if ((segattrs.fseq =
			    strtol (Coptarg, &dp, 10)) <= 0 || *dp != '\0') {
				fprintf (stderr,
				    "invalid file sequence number %s\n", Coptarg);
				errflg++;
			}
			break;
		case 's':
			segattrs.segsize = strtou64 (Coptarg);
			break;
		case 'V':
			if (strlen (Coptarg) > CA_MAXVIDLEN) {
				fprintf (stderr,
				    "invalid vid %s\n", Coptarg);
				errflg++;
			} else
				strcpy (tape.vid, Coptarg);
				strcpy (segattrs.vid, Coptarg);
			break;
		case 'v':
			if (strlen (Coptarg) > CA_MAXVSNLEN) {
				fprintf (stderr,
				    "invalid vsn %s\n", Coptarg);
				errflg++;
			} else
				strcpy (tape.vsn, Coptarg);
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	return (errflg ? USERR : 0);
}

dir2argv(inpdir, argvp)
char *inpdir;
char ***argvp;
{
	char **argv;
	int c;
	int nargs;
	char *p;
	int parm;

	nargs = 1;
	p = inpdir;
	parm = 0;
	while (c = *p++) {
		if (c == ' ' || c == '\t') {
			parm = 0;
		} else if (!parm) {
				parm++;
				nargs++;
		}
	}
	argv = (char **) malloc ((nargs + 1) * sizeof(char *));
	argv[0] = "tapetohsm";
	nargs = 1;
	p = inpdir;
	parm = 0;
	while (c = *p++) {
		if (c == ' ' || c == '\t') {
			if (parm) {
				parm = 0;
				*(p - 1) = '\0';
			}
		} else if (!parm) {
			parm++;
			argv[nargs++] = p - 1;
		}
	}
	argv[nargs] = NULL;
	*argvp = argv;
	return (nargs);
}

procfile()
{
	int errflg = 0;
	struct Cns_fileid file_uniqueid;
	char tmpbuf[21];
	char tmpbuf2[21];

	if (! hsm_path) {
		fprintf (stderr, "hsm_path is mandatory\n");
		return (USERR);
	}

	/* create the HSM file entry in the Name Server */

	if (Cns_creatx (hsm_path, 0666, &file_uniqueid) < 0) {
		fprintf (stderr, "%s: %s\n", hsm_path, sstrerror(serrno));
		return (USERR);
	}

	/* set filesize */

	if (verbose)
		printf ("Cns_setfsize (NULL, %s, %s)\n",
		    u64tostr (file_uniqueid.fileid, tmpbuf, 0),
		    u64tostr (segattrs.segsize, tmpbuf2, 0));
	if (Cns_setfsize (NULL, &file_uniqueid, segattrs.segsize) < 0) {
		fprintf (stderr, "%s: %s\n", hsm_path, sstrerror(serrno));
		return (USERR);
	}

	/* set segment attributes */

	if (verbose)
		printf ("Cns_setsegattrs (NULL, %s, 1, %s %s %d)\n",
		    u64tostr (file_uniqueid.fileid, tmpbuf, 0),
		    u64tostr (segattrs.segsize, tmpbuf2, 0),
		    segattrs.vid, segattrs.fseq);
	if (Cns_setsegattrs (NULL, &file_uniqueid, 1, &segattrs) < 0) {
		fprintf (stderr, "%s: %s\n", hsm_path, sstrerror(serrno));
		return (USERR);
	}
	if (segattrs.fseq > maxfseq)
		maxfseq = segattrs.fseq;
	BytesWritten += segattrs.segsize;
	hsm_path = NULL;
	segattrs.fseq = 0;
	segattrs.segsize = 0;
	return (0);
}

procinpdir(s)
FILE *s;
{
	char **argv_d;
	char buf[256];
	int c;
	char *dp;
	int errflg = 0;
	int n;
	int nargs;
	char *p;

	/* check and store the directives in an array */

	while (fgets (buf, sizeof(buf), s) != NULL) {
		if (buf[strlen (buf)-1] != '\n') {
			fprintf (stderr, "directive line too long\n");
			return (USERR);
		} else
			buf[strlen (buf)-1] = '\0';
		if ((n = strspn (buf, " \t")) == strlen (buf)) continue; /* blank line */
		p = buf + n;
		if (*p == '#') continue;	/* comment line */
		nargs = dir2argv (p, &argv_d);
		if (chkopt (nargs, argv_d, 0)) {
			errflg++;
			continue;
		}
		if (procfile ())
			errflg++;
	}
	return (errflg ? USERR : 0);
}

#if TMS
tmscheck(vid, vsn, dgn, den, lbl)
char *vid;
char *vsn;
char *dgn;
char *den;
char *lbl;
{
	int c, j;
	int errflg = 0;
	char *p;
	int repsize;
	int reqlen;
	char tmrepbuf[132];
	static char tmsden[6] = "     ";
	static char tmsdgn[7] = "      ";
	static char tmslbl[3] = "  ";
	char tmsreq[80];
	static char tmsvsn[7] = "      ";

	sprintf (tmsreq, "VIDMAP %s QUERY (GENERIC SHIFT MESSAGE", vid);
	reqlen = strlen (tmsreq);
	while (1) {
		repsize = sizeof(tmrepbuf);
		c = sysreq ("TMS", tmsreq, &reqlen, tmrepbuf, &repsize);
		switch (c) {
		case 0:
			break;
		case 8:
		case 100:
		case 312:
		case 315:
			fprintf (stderr, "%s\n", tmrepbuf);
			errflg++;
			break;
		case 12:
			fprintf (stderr, "access to volume %s denied by TMS\n", vid);
			errflg++;
			break;
		default:
			sleep (60);
			continue;
		}
		break;
	}
	if (errflg) return (USERR);
	strncpy (tmsvsn, tmrepbuf, 6);
	for  (j = 0; tmsvsn[j]; j++)
		if (tmsvsn[j] == ' ') break;
	tmsvsn[j] = '\0';
	if (*vsn) {
		if (strcmp (vsn, tmsvsn)) {
			fprintf (stderr,
			    "parameter inconsistency with TMS for vid %s: %s<->%s\n",
			    vid, vsn, tmsvsn);
			errflg++;
		}
	}

	strncpy (tmsdgn, tmrepbuf+ 25, 6);
	for  (j = 0; tmsdgn[j]; j++)
		if (tmsdgn[j] == ' ') break;
	tmsdgn[j] = '\0';
	if (strcmp (tmsdgn, "CT1") == 0) strcpy (tmsdgn, "CART");
	if (*dgn) {
		if (strcmp (dgn, tmsdgn) != 0) {
			fprintf (stderr,
			    "parameter inconsistency with TMS for vid %s: %s<->%s\n",
			    vid, dgn, tmsdgn);
			errflg++;
		}
	}

	p = tmrepbuf + 32;
	while (*p == ' ') p++;
	j = tmrepbuf + 40 - p;
	strncpy (tmsden, p, j);
	tmsden[j] = '\0';
	if (*den) {
		if (strcmp (den, tmsden) != 0) {
			fprintf (stderr,
			    "parameter inconsistency with TMS for vid %s: %s<->%s\n",
			    vid, den, tmsden);
			errflg++;
		}
	}

	tmslbl[0] = tmrepbuf[74] - 'A' + 'a';
	tmslbl[1] = tmrepbuf[75] - 'A' + 'a';
	if (*lbl) {
		if (strcmp (lbl, "blp") && strcmp (lbl, tmslbl)) {
			fprintf (stderr,
			    "parameter inconsistency with TMS for vid %s: %s<->%s\n",
			    vid, lbl, tmslbl);
			errflg++;
		}
	}
	return (errflg);
}
#endif

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s",
	    "[-d density] -g dgn [-i] [-l lbltype] [-M hsm_path_name]\n",
	    "\t[-P pool_name] [-q file_sequence_number] [-s size] -V vid [-v vsn]\n",
	    "\t[--ml media_letter] --mo model [--verbose]\n");
}
