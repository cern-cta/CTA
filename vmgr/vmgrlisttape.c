/*
 * Copyright (C) 2000-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrlisttape.c,v $ $Revision: 1.15 $ $Date: 2001/07/24 07:57:13 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	vmgrlisttape - query a given volume or list all existing tapes */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "serrno.h"
#include "u64subr.h"
#include "vmgr.h"
#include "vmgr_api.h"
extern	char	*optarg;
extern	int	optind;
main(argc, argv)
int argc;
char **argv;
{
	int c;
	char density[CA_MAXDENLEN+1];
	char dgn[CA_MAXDGNLEN+1];
	int errflg = 0;
	int flags;
	int free_space;
	char lbltype[3];
	vmgr_list list;
	struct vmgr_tape_info *lp;
	char manufacturer[CA_MAXMANUFLEN+1];
	char media_letter[CA_MAXMLLEN+1];
	char model[CA_MAXMODELLEN+1];
	int nbfiles;
	char pool_name[CA_MAXPOOLNAMELEN+1];
	int rcount;
	time_t rtime;
	char sn[CA_MAXSNLEN+1];
	int status;
	char vid[CA_MAXVIDLEN+1];
	char vsn[CA_MAXVSNLEN+1];
	int wcount;
	time_t wtime;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	int xflag = 0;

	pool_name[0]= '\0';
	vid[0]= '\0';
        while ((c = getopt (argc, argv, "P:V:x")) != EOF) {
                switch (c) {
		case 'P':
			if (strlen (optarg) <= CA_MAXPOOLNAMELEN)
				strcpy (pool_name, optarg);
			else {
				fprintf (stderr, "%s\n", strerror(EINVAL));
				errflg++;
			}
                        break;
		case 'V':
			if (strlen (optarg) <= CA_MAXVIDLEN)
				strcpy (vid, optarg);
			else {
				fprintf (stderr, "%s\n", strerror(EINVAL));
				errflg++;
			}
                        break;
		case 'x':
			xflag = 1;
			break;
                case '?':
                        errflg++;
                        break;
                default:
                        break;
                }
        }
        if (optind < argc) {
                errflg++;
        }
        if (errflg) {
                fprintf (stderr, "usage: %s %s", argv[0],
		    "[-P pool_name] [-V vid] [-x]\n");
                exit (USERR);
        }
 
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, VMG52);
		exit (SYERR);
	}
#endif
	if (*vid) {
		if (vmgr_querytape (vid, vsn, dgn, density, lbltype, model,
		    media_letter, manufacturer, sn, pool_name, &free_space,
		    &nbfiles, &rcount, &wcount, &rtime, &wtime, &status) < 0) {
			fprintf (stderr, "vmgrlisttape %s: %s\n", vid,
			    (serrno == ENOENT) ? "No such tape" : sstrerror(serrno));
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (USERR);
		}
		listentry (vid, vsn, dgn, density, lbltype, model,
		    media_letter, manufacturer, sn, pool_name, free_space,
		    nbfiles, rcount, wcount, rtime, wtime, status, xflag);
	} else {
		flags = VMGR_LIST_BEGIN;
		while ((lp = vmgr_listtape (pool_name, flags, &list)) != NULL) {
			listentry (lp->vid, lp->vsn, lp->dgn, lp->density,
			    lp->lbltype, lp->model, lp->media_letter,
			    lp->manufacturer, lp->sn, lp->poolname,
			    lp->estimated_free_space, lp->nbfiles, lp->rcount,
			    lp->wcount, lp->rtime, lp->wtime, lp->status, xflag);
			flags = VMGR_LIST_CONTINUE;
		}
		(void) vmgr_listtape (pool_name, VMGR_LIST_END, &list);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}

listentry(vid, vsn, dgn, density, lbltype, model, media_letter, manufacturer,
sn, pool_name, free_space, nbfiles, rcount, wcount, rtime, wtime, status, xflag)
char *vid;
char *vsn;
char *dgn;
char *density;
char *lbltype;
char *model;
char *media_letter;
char *manufacturer;
char *sn;
char *pool_name;
int free_space;
int nbfiles;
int rcount;
int wcount;
time_t rtime;
time_t wtime;
int status;
int xflag;
{
	time_t ltime;
	char p_stat = '\0';
	struct tm *tm;
	char tmpbuf[9];
	u_signed64 u64;

	u64 = ((u_signed64) free_space) * 1024;
	printf ("%-6s %-6s %-6s %-8s %-2s ",
	    vid, vsn, dgn, density, lbltype);
	if (! xflag) {
		printf ("%-15s %-8sB ", pool_name, u64tostru (u64, tmpbuf, 8));
		ltime = (wtime < rtime) ? rtime : wtime;
		if (ltime) {
			tm = localtime (&ltime);
			printf ("%04d%02d%02d ",
			    tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
		} else
			printf ("00000000 ");
	} else {
		printf ("%-6s %-2s %-12s %-25s %-15s %-8sB %6d %5d %5d ",
		    model, media_letter, manufacturer, sn, pool_name,
		    u64tostru (u64, tmpbuf, 8), nbfiles, rcount, wcount);
		if (rtime) {
			tm = localtime (&rtime);
			printf ("%04d%02d%02d ",
			    tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
		} else
			printf ("00000000 ");
		if (wtime) {
			tm = localtime (&wtime);
			printf ("%04d%02d%02d ",
			    tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
		} else
			printf ("00000000 ");
	}
	if (status & TAPE_FULL) {
		printf ("FULL");
		p_stat = '|';
	}
	if (status & TAPE_BUSY) {
		printf (p_stat ? "|BUSY" : "BUSY");
		p_stat = '|';
	}
	if (status & TAPE_RDONLY) {
		printf (p_stat ? "|RDONLY" : "RDONLY");
		p_stat = '|';
	}
	if (status & EXPORTED) {
		printf (p_stat ? "|EXPORTED" : "EXPORTED");
		p_stat = '|';
	}
	if (status & DISABLED) {
		printf (p_stat ? "|DISABLED" : "DISABLED");
		p_stat = '|';
	}
	if (status & ARCHIVED) {
		printf (p_stat ? "|ARCHIVED" : "ARCHIVED");
		p_stat = '|';
	}
	printf ("\n");
}
