/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrlisttape.c,v $ $Revision: 1.16 $ $Date: 2002/02/07 06:12:58 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
	char dgn[CA_MAXDGNLEN+1];
	int errflg = 0;
	int flags;
	vmgr_list list;
	struct vmgr_tape_info *lp;
	char pool_name[CA_MAXPOOLNAMELEN+1];
	struct vmgr_tape_info *tape_info;
	char vid[CA_MAXVIDLEN+1];
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
	flags = VMGR_LIST_BEGIN;
	serrno = 0;
	while ((lp = vmgr_listtape (vid, pool_name, flags, &list)) != NULL) {
		listentry (lp, xflag);
		flags = VMGR_LIST_CONTINUE;
	}
	if (serrno)
		fprintf (stderr, "vmgrlisttape: %s\n",
		    (serrno == ENOENT) ? "No such tape" : sstrerror(serrno));
	else
		(void) vmgr_listtape (vid, pool_name, VMGR_LIST_END, &list);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}

listentry(lp, xflag)
struct vmgr_tape_info *lp;
int xflag;
{
	time_t ltime;
	char p_stat = '\0';
	struct tm *tm;
	char tmpbuf[9];
	u_signed64 u64;

	u64 = ((u_signed64) lp->estimated_free_space) * 1024;
	if (lp->nbsides > 1)
		printf ("%-6s/%d ", lp->vid, lp->side);
	else
		printf ("%-6s   ", lp->vid);
	printf ("%-6s %-8s %-8s %-3s ",
	    lp->vsn, lp->library, lp->density, lp->lbltype);
	if (! xflag) {
		printf ("%-15s %-8sB ", lp->poolname, u64tostru (u64, tmpbuf, 8));
		ltime = (lp->wtime < lp->rtime) ? lp->rtime : lp->wtime;
		if (ltime) {
			tm = localtime (&ltime);
			printf ("%04d%02d%02d ",
			    tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
		} else
			printf ("00000000 ");
	} else {
		printf ("%-6s %-2s %-12s %-25s %-15s ",
		    lp->model, lp->media_letter, lp->manufacturer, lp->sn,
		    lp->poolname);
		if (lp->etime) {
			tm = localtime (&lp->etime);
			printf ("%04d%02d%02d ",
			    tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
		} else
			printf ("00000000 ");
		printf ("%-8sB %6d %5d %5d %-10s %-10s %10d %10d ",
		    u64tostru (u64, tmpbuf, 8), lp->nbfiles, lp->rcount,
		    lp->wcount, lp->rhost, lp->whost, lp->rjid, lp->wjid);
		if (lp->rtime) {
			tm = localtime (&lp->rtime);
			printf ("%04d%02d%02d ",
			    tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
		} else
			printf ("00000000 ");
		if (lp->wtime) {
			tm = localtime (&lp->wtime);
			printf ("%04d%02d%02d ",
			    tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
		} else
			printf ("00000000 ");
	}
	if (lp->status & TAPE_FULL) {
		printf ("FULL");
		p_stat = '|';
	}
	if (lp->status & TAPE_BUSY) {
		printf (p_stat ? "|BUSY" : "BUSY");
		p_stat = '|';
	}
	if (lp->status & TAPE_RDONLY) {
		printf (p_stat ? "|RDONLY" : "RDONLY");
		p_stat = '|';
	}
	if (lp->status & EXPORTED) {
		printf (p_stat ? "|EXPORTED" : "EXPORTED");
		p_stat = '|';
	}
	if (lp->status & DISABLED) {
		printf (p_stat ? "|DISABLED" : "DISABLED");
		p_stat = '|';
	}
	if (lp->status & ARCHIVED) {
		printf (p_stat ? "|ARCHIVED" : "ARCHIVED");
		p_stat = '|';
	}
	printf ("\n");
}
