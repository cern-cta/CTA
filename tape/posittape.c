/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: posittape.c,v $ $Revision: 1.6 $ $Date: 2000/01/22 08:20:17 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include "Ctape.h"
#include "serrno.h"
posittape(tapefd, path, devtype, lblcode, mode, cfseq, fid, filstat, fsec, fseq,
	den, flags, Qfirst, Qlast, vol1, hdr1, hdr2)
int tapefd;
char *path;
char *devtype;
int lblcode;
int mode;
int *cfseq;
char *fid;
int filstat;
int fsec;
int fseq;
int den;
int flags;
int Qfirst;
int Qlast;
char *vol1, *hdr1, *hdr2;
{
	char buf[7];
	int c;
	time_t current_time;
	extern char *dvrname;
	char func[16];
	struct tm *localtime(), *tm;
	int n;
	int nohdr2;
	char *p;
	int pfseq;
	int rewritetm = 1;	/* An Exabyte 8200 must be positionned on the
			BOT side of a long filemark before starting to write */
	char sfseq[7];
	char tpfid[CA_MAXFIDLEN+1];

	ENTRY (posittape);
	c = 0;
	sprintf (sfseq, "%d", fseq);
	pfseq = *cfseq;		/* save current file sequence number */
	if (Qfirst && fseq > 0) fseq += Qfirst - 1;
	if (strcmp (devtype, "8200") && den != D8200 && den != D8200C)
		rewritetm = 0;
	if (lblcode == NL || lblcode == BLP) {
		if (flags & NOPOS) {	/* tape is already positionned */
			if (filstat == CHECK_FILE) {	/* must check if the file exist */
				if ((c = readlbl (tapefd, path, vol1)) < 0) goto reply;
				if (c >= 2) {
					c = ETFSQ;
					goto reply;
				}
				if ((c = skiptpfb (tapefd, path, 1))) goto reply;
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
			}
			(*cfseq)++;
			goto reply;
		}
		if ((c = rwndtape (tapefd, path))) goto reply;
		*cfseq = 1;
		if (fseq == 1 && filstat == NEW_FILE) goto reply;
#if defined(ADSTAR) || (defined(__osf__) && defined(__alpha)) || defined(IRIX64) || defined(linux)
#if defined(ADSTAR)
		if (strcmp (dvrname, "Atape") == 0 &&
		    strcmp (devtype, "3590") == 0 &&
#endif
#if defined(__osf__) && defined(__alpha)
		if ((strncmp (devtype, "DLT", 3) == 0 ||
		    strcmp (devtype, "9840") == 0 ||
		    strcmp (devtype, "SD3") == 0) &&
#endif
#if defined(IRIX64) || defined(linux)
                if ((strncmp (devtype, "DLT", 3) == 0 ||
		    strcmp (devtype, "3590") == 0 ||
		    strcmp (devtype, "9840") == 0 ||
                    strcmp (devtype, "SD3") == 0) &&
#endif
		    (fseq > 2 || fseq == -1)) {	/* fast positionning */
			if (fseq > 0)
				n = fseq - 1;
			else {		/* -q n */
				if ((c = readlbl (tapefd, path, vol1)) < 0) goto reply;
				if (c > 1) {	/* blank tape or only TM(s) */
					c = rwndtape (tapefd, path);
					goto reply;
				}
				n = 30000;	/* arbitrary large count */
			}
			tplogit (func, "trying to skip over %d files\n", n);
			if ((c = skiptpfff (tapefd, path, n)) < 0) goto reply;
			if (c == 0) {
				*cfseq = fseq;
				if ((c = readlbl (tapefd, path, vol1)) < 0) goto reply;
				if (c < 2) {	/* the requested file exists */
					if ((c = skiptpfb (tapefd, path, 1))) goto reply;
					if ((c = skiptpff (tapefd, path, 1))) goto reply;
					goto finalpos;
				} else {
					if (c == 2) {	/* tape mark */
						if (filstat != NEW_FILE) {
							c = ETFSQ;
							goto reply;
						}
						c = skiptpfb (tapefd, path, 1);
						goto reply;
					} else		/* blank tape */
						(*cfseq)--;
				}
			} else {
				*cfseq = n - c;
			}
			if (*cfseq == 1) {	/* 1 datafile + 1 TM */
				*cfseq = 2;
				c = 3;
			} else {
				if ((c = skiptpfb (tapefd, path, 2))) goto reply;
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
				if ((c = readlbl (tapefd, path, vol1)) < 0) goto reply;
				if (c < 2) {	/* only one tape mark at EOI */
					if ((c = skiptpff (tapefd, path, 1))) goto reply;
					c = 3;
					(*cfseq)++;
				}
			}
			if (fseq > 0 && fseq != *cfseq) {
				if (filstat != NEW_FILE)
					usrmsg (func, TP024, sfseq);
				else
					usrmsg (func, TP045, fseq, *cfseq-1);
				c = ENOENT;
				goto reply;
			}
			if (filstat != NEW_FILE) {
				c = ETFSQ;
				goto reply;
			}
			c = 3 - c;
#if defined(SOLARIS) || defined(linux)
			if (c > 0)
#endif
				c = skiptpfb (tapefd, path, c);
			goto reply;
		}
#endif
		while (fseq >= *cfseq || fseq == -1) {
			if ((c = readlbl (tapefd, path, vol1)) < 0 &&
				c != -ETPARIT) goto reply;
			if (c > 1) {	/* double tape mark or blank tape found */
				if (fseq == -1) fseq = *cfseq;
				if (fseq != *cfseq) {
					if (c == 2 && (flags & IGNOREEOI)) {
						(*cfseq)++;
						continue;	/* ignoreeoi */
					}
					if (filstat != NEW_FILE)
						usrmsg (func, TP024, sfseq);
					else
						usrmsg (func, TP045, fseq, *cfseq-1);
					c = ENOENT;
					goto reply;
				}
				if (filstat != NEW_FILE) {
					c = ETFSQ;
					goto reply;
				}
				if (fseq == 1) {
					c = rwndtape (tapefd, path);
				} else {
					c = 3 + rewritetm - c;
#if defined(SOLARIS) || defined(linux)
					if (c > 0)
#endif
						c = skiptpfb (tapefd, path, c);
				}
				goto reply;
			}
			if (fseq == *cfseq) {	/* the requested file exists */
				/* reposition the tape to the beginning of file */
				if (fseq == 1) {
					if ((c = rwndtape (tapefd, path))) goto reply;
				} else {
					if ((c = skiptpfb (tapefd, path, 1))) goto reply;
					if (mode == WRITE_DISABLE || filstat == APPEND || ! rewritetm)
						if ((c = skiptpff (tapefd, path, 1))) goto reply;
				}
				break;
			}
			if ((c = skiptpff (tapefd, path, 1))) goto reply;
			(*cfseq)++;
		}
	} else {	/* AL or SL */
		if (flags & NOPOS && cfseq && filstat == NEW_FILE) {	/* already positionned */
			(*cfseq)++;
			return (0);
		}
		if (*cfseq == 0 || fseq == 1 || fsec > 1 || 
		    (fseq == *cfseq && mode == WRITE_ENABLE)) {
			if ((c = rwndtape (tapefd, path))) goto reply;
			if ((c = readlbl (tapefd, path, vol1)) < 0) goto reply;
			if ((c = readlbl (tapefd, path, hdr1)) < 0) goto reply;
			if (c) {
				c = ETLBL;
				goto reply;
			}
			if (lblcode == SL) ebc2asc (hdr1, 80);
			if (strncmp (hdr1, "HDR1", 4)) {
				c = ETLBL;
				goto reply;
			}
			sscanf (hdr1 + 31, "%4d", cfseq);
			if (fsec > 1)	{ /* nth file section in multivolume set */
				if (*cfseq != fseq && filstat != NEW_FILE) {
					c = ETLBL;
					goto reply;
				} else
					*cfseq = fseq;
			} else if (*cfseq != 1) {
				if (fseq != 1 || filstat != NEW_FILE) {
					c = ETLBL;
					goto reply;
				} else {
					*cfseq = 1;
				}
			}
			if (fseq == -2 ) {	/* position by fid */
				strncpy (tpfid, hdr1 + 4, 17);
				tpfid[17] = '\0';
				if ((p = strchr (tpfid, ' ')) != NULL) *p = '\0';
				if (strcmp (tpfid, fid) == 0) fseq = *cfseq;
			}
			if (fseq != *cfseq) {
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
				if (fseq == -1) {	/* -q n */
					if ((c = readlbl (tapefd, path, hdr1)) < 0)
						goto reply;
					if (c > 1) {	/* tape is prelabelled only */
						goto chkexpdat;
					}
				}
			}
		} else if (fseq == *cfseq) {
			if ((c = skiptpfb (tapefd, path, 3))) goto reply;
			if ((c = skiptpff (tapefd, path, 1))) goto reply;
			if ((c = readlbl (tapefd, path, hdr1)) < 0) goto reply;
			if (lblcode == SL) ebc2asc (hdr1, 80);
			if (strncmp (hdr1, "EOF1", 4) == 0) {
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
				if ((c = readlbl (tapefd, path, hdr1)) < 0) goto reply;
				if (lblcode == SL) ebc2asc (hdr1, 80);
			}
			sscanf (hdr1 + 31, "%4d", cfseq);
			if (fseq > *cfseq)
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
		}
#if defined(ADSTAR) || (defined(__osf__) && defined(__alpha)) || defined(IRIX64) || defined(linux)
#if defined(ADSTAR)
		if (strcmp (dvrname, "Atape") == 0 &&
		    strcmp (devtype, "3590") == 0 &&
#endif
#if defined(__osf__) && defined(__alpha)
		if ((strncmp (devtype, "DLT", 3) == 0 ||
		    strcmp (devtype, "9840") == 0 ||
		    strcmp (devtype, "SD3") == 0) &&
#endif
#if defined(IRIX64) || defined(linux)
                if ((strncmp (devtype, "DLT", 3) == 0 ||
                    strcmp (devtype, "3590") == 0 ||
		    strcmp (devtype, "9840") == 0 ||
                    strcmp (devtype, "SD3") == 0) &&
#endif
		    (fseq > *cfseq + 1 || fseq == -1 ||
		    (fseq == -2 && Qfirst > *cfseq + 1))) { /* fast positionning */
			if (fseq > 0)
				n = (fseq - *cfseq - 1) * 3 + 1;
			else if (fseq == -2)
				n = (Qfirst - *cfseq - 1) * 3 + 1;
			else
				n = 30000;	/* arbitrary large count */
			tplogit (func, "trying to skip over %d files\n", n);
			if ((c = skiptpfff (tapefd, path, n)) < 0) goto reply;
			if (c > 0) {
				if ((c = skiptpfb (tapefd, path, 2))) goto reply;
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
				if ((c = readlbl (tapefd, path, hdr1)) < 0) goto reply;
				if (c == 2) {	/* two tape marks at EOI */
					if ((c = skiptpfb (tapefd, path, 3))) goto reply;
					if ((c = skiptpff (tapefd, path, 1))) goto reply;
					if ((c = readlbl (tapefd, path, hdr1)) < 0) goto reply;
				}
				if (c) {
					c = ETLBL;
					goto reply;
				}
				if (lblcode == SL) ebc2asc (hdr1, 80);
				if (strncmp (hdr1, "EOF1", 4)) {
					c = ETLBL;
					goto reply;
				}
				sscanf (hdr1 + 31, "%4d", cfseq);
				if (fseq > 0) {
					if (filstat != NEW_FILE)
						usrmsg (func, TP024, sfseq);
					else
						usrmsg (func, TP045, fseq, *cfseq);
					c = ENOENT;
					goto reply;
				}
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
				(*cfseq)++;
				goto reply;
			} else
				*cfseq = fseq - 1;
		} else
#endif
			if ((pfseq == 0 || filstat == CHECK_FILE) && (fseq > *cfseq || fseq < 0))
				if ((c = skiptpff (tapefd, path, 1))) goto reply;

		while (fseq > *cfseq || fseq < 0) {
			if ((c = readlbl (tapefd, path, hdr1)) < 0) goto reply;
			if (c == 0) {
				if (lblcode == SL) ebc2asc (hdr1, 80);
				if (strncmp (hdr1, "EOF1", 4) == 0) {
					if ((c = skiptpff (tapefd, path, 1))) goto reply;
					if ((c = readlbl (tapefd, path, hdr1)) < 0) goto reply;
					if (c == 0 && lblcode == SL) ebc2asc (hdr1, 80);
				}
			}
			if (c == 1) {
				c = ETLBL;
				goto reply;
			}
			if (c > 1) {	/* double tape mark or blank tape found */
				(*cfseq)++;
				if (fseq == -1) fseq = *cfseq;
				if (fseq != *cfseq) {
					if (fseq == -2)		/* -q u */
						usrmsg (func, TP024, fid);
					else
						if (filstat != NEW_FILE)
							usrmsg (func, TP024, sfseq);
						else
							usrmsg (func, TP045, fseq, *cfseq-1);
					c = ENOENT;
					goto reply;
				}
				if (filstat != NEW_FILE) {
					c = ETFSQ;
					goto reply;
				}
				c = 3 + rewritetm - c;
#if defined(SOLARIS) || defined(linux)
				if (c > 0)
#endif
					c = skiptpfb (tapefd, path, c);
				goto reply;
			}
			if (strncmp (hdr1, "HDR1", 4)) {
				c = ETLBL;
				goto reply;
			}
			sscanf (hdr1 + 31, "%4d", cfseq);
			if (fseq == -2 &&	/* position by fid */
			    (Qfirst == 0 || *cfseq >= Qfirst)) {
				strncpy (tpfid, hdr1 + 4, 17);
				tpfid[17] = '\0';
				if ((p = strchr (tpfid, ' ')) != NULL) *p = '\0';
				if (strcmp (tpfid, fid) == 0) fseq = *cfseq;
			}
			if (fseq == *cfseq) break;
			if (fseq > 0 && fseq < *cfseq) {
				c = ETLBL;
				goto reply;
			}
			if (Qlast && *cfseq >= Qlast) {
				usrmsg (func, TP024, fid);
				c = ENOENT;
				goto reply;
			}
			if ((c = skiptpff (tapefd, path, 3))) goto reply;
		}
		if (fseq < *cfseq) {
			skiptpfb (tapefd, path, (*cfseq - fseq + 1) * 3);
			if ((c = skiptpff (tapefd, path, 1))) goto reply;
			if ((c = readlbl (tapefd, path, hdr1)) != 0) goto reply;
			if (lblcode == SL) ebc2asc (hdr1, 80);
			if (strncmp (hdr1, "EOF1", 4) == 0) {
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
				if ((c = readlbl (tapefd, path, hdr1)) < 0) goto reply;
				if (lblcode == SL) ebc2asc (hdr1, 80);
			}
			sscanf (hdr1 + 31, "%4d", cfseq);
		}
		tplogit (func, "hdr1 = %s\n", hdr1);
		nohdr2 = 0;
		if ((c = readlbl (tapefd, path, hdr2)) < 0) goto reply;
		if (c == 0) {
			if (lblcode == SL) ebc2asc (hdr2, 80);
			if (strncmp (hdr2, "HDR2", 4) != 0) {
				c = ETLBL;
				goto reply;
			}
			tplogit (func, "hdr2 = %s\n", hdr2);
		}
		if (c == 1) {
			c = ETLBL;
			goto reply;
		}
		if (c == 2) {	/* tape mark found */
			hdr2[0] = '\0';
			nohdr2 = 1;
			c = 0;
		}
		if (filstat != NEW_FILE && *fid) {
			strncpy (tpfid, hdr1 + 4, 17);
			tpfid[17] = '\0';
			if ((p = strchr (tpfid, ' ')) != NULL) *p = '\0';
			if (strcmp (tpfid, fid)) {
				usrmsg (func, TP044, fid, fseq, tpfid);
				c = ENOENT;
				goto reply;
			}
		}
chkexpdat:
		if (mode == WRITE_ENABLE && filstat != APPEND) {
			/* check expiration date and position to overwrite labels */
			time (&current_time);
			tm = localtime (&current_time);
			sprintf (buf, "%c%.2d%.3d", tm->tm_year / 100 ? '0' : ' ',
				tm->tm_year % 100, tm->tm_yday + 1);
			if (strncmp (hdr1 + 47, "000000", 6) != 0 &&
			    strncmp (hdr1 + 47, buf, 6) > 0) {
				c = EACCES;
				goto reply;
			}
			if (*cfseq == 1 || fsec > 1) {
				if ((c = rwndtape (tapefd, path))) goto reply;
			} else {
				if ((c = skiptpfb (tapefd, path, nohdr2 ? 2 : 1)))
					goto reply;
				if (! rewritetm)
					if ((c = skiptpff (tapefd, path, 1))) goto reply;
			}
			goto reply;
		} else {	/* skip to data */
			if (! nohdr2)
				if ((c = skiptpff (tapefd, path, 1))) goto reply;
		}
	}
finalpos:
	if (filstat == APPEND) {	/* append mode, position after data */
		if ((c = skiptpff (tapefd, path, 1))) goto reply;
		if ((c = skiptpfb (tapefd, path, 1))) goto reply;
	}
reply:
	switch (c) {
	case 0:
		break;
	case EACCES:
		usrmsg (func, TP028);
		break;
	case ETFSQ:
		if (fseq == -2)		/* -q u */
			usrmsg (func, TP024, fid);
		else
			usrmsg (func, TP024, sfseq);
		break;
	case ENOENT:
		break;
	case ETLBL:
		usrmsg (func, TP025);
		break;
	}
	RETURN (c);
}
