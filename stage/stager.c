/*
 * $Id: stager.c,v 1.9 2000/02/11 11:06:58 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager.c,v $ $Revision: 1.9 $ $Date: 2000/02/11 11:06:58 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#if _AIX && _IBMR2
#include <sys/select.h>
#endif
#include <sys/wait.h>
#if defined(ultrix) || (defined(sun) && !defined(SOLARIS))
#include <sys/resource.h>
#endif
#define RFIO_KERNEL
#include "rfio.h"
#include "stage.h"
#include "osdep.h"

void checkovlstatus _PROTO((int, int));

#if !defined(linux)
extern char *sys_errlist[];
#endif
char func[16];
int maxfds;
int nbcat_ent;
int ovl_status = -1;
int pid;
fd_set readfd, readmask;
int reqid;
int rpfd;
#if (defined(_AIX) && defined(_IBMR2)) || defined(SOLARIS) || defined(IRIX5) || (defined(__osf__) && defined(__alpha)) || defined(linux)
struct sigaction sa;
#endif
struct stgcat_entry *stce;      /* end of stage catalog */
struct stgcat_entry *stcs;      /* start of stage catalog */
main(argc, argv)
int argc;
char **argv;
{
	int Aflag;
	char arg_blksize[7];
	char *arg_fid = NULL;
	char arg_filstat[2], arg_fseq[512];
	char arg_lrecl[7], arg_retentd[5];
	char *arg_nread = NULL;
	char *arg_size = NULL;
	char arg_Z[14+CA_MAXHOSTNAMELEN + 1], arg_vid[MAXVSN*7], arg_vsn[MAXVSN*7];
	char buf[REPBUFSZ-12];
	int i, l;
	int cmdargc;
	char **cmdargv;
	char cmdname[9];
	fseq_elem *fseq_list;
	char hostname[CA_MAXHOSTNAMELEN + 1];
	int key;
	int n1, n2;
	int nbtpf;
	int nretry;
	char *p, *q;
	int pfd[2];
	char progfullpath[MAXPATH];
	int samefid;
	int samesize;
	void stagekilled();
	struct stgcat_entry *stcp;
	struct stgcat_entry stgreq;
	struct timeval timeval;
	char trailing;
	void wait4child();

	strcpy (func, "stager");
	stglogit (func, "function entered\n");

#if defined(ultrix) || (defined(sun) && (!defined(SOLARIS) || defined(SOLARIS25))) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(IRIX6)
	maxfds = getdtablesize();
#else
	maxfds = _NFILE;
#endif
	reqid = atoi (argv[1]);
	key = atoi (argv[2]);
	rpfd = atoi (argv[3]);
	nbcat_ent = atoi (argv[4]);
	nretry = atoi (argv[5]);
	Aflag = atoi (argv[6]);

	stcs = (struct stgcat_entry *) malloc (nbcat_ent * sizeof(struct stgcat_entry));
	stcp = stcs;
	while ((l = read (0, &stgreq, sizeof(stgreq)))) {
		if (l == sizeof(stgreq)) {
			memcpy (stcp, &stgreq, sizeof(stgreq));
			stcp++;
		}
	}
	stce = stcp;
	close (0);

	signal (SIGINT, stagekilled);
	if (nretry) sleep (RETRYI);

	gethostname (hostname, CA_MAXHOSTNAMELEN + 1);

	if ((stcs->t_or_d == 'm') || (stcs->t_or_d == 'd')) {
		int exit_code;

		exit_code = filecopy (stcs, key, hostname);
		stglogit (func, "filecopy exiting with status %x\n", exit_code & 0xFFFF);
		ovl_status = (exit_code & 0xFF) ? SYERR : ((exit_code >> 8) & 0xFF);
		exit (ovl_status);
	}

#if defined(ultrix) || (defined(sun) && !defined(SOLARIS)) || (defined(_AIX) && defined(_IBMESA))
	signal (SIGCHLD, wait4child);
#else
#if (defined(sgi) && !defined(IRIX5)) || defined(hpux)
	signal (SIGCLD, wait4child);
#else
#if (defined(_AIX) && defined(_IBMR2)) || defined(SOLARIS) || defined(IRIX5) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	sa.sa_handler = wait4child;
	sa.sa_flags = SA_RESTART;
	sigaction (SIGCHLD, &sa, NULL);
#endif
#endif
#endif

	/* build the command */

	cmdargc = 0;
	cmdargv = (char **) malloc ((nbcat_ent + 43) * sizeof(char *));
	strcpy (cmdname, (stcs->status == STAGEWRT || stcs->status == STAGEPUT) ?
		"tpwrite" : "tpread");
	cmdargv[cmdargc++] = cmdname;
	if (Aflag) {
		cmdargv[cmdargc++] = "-A";
		cmdargv[cmdargc++] = "deferred";
	}
	if (stcs->blksize) {
		cmdargv[cmdargc++] = "-b";
		sprintf (arg_blksize, "%d", stcs->blksize);
		cmdargv[cmdargc++] = arg_blksize;
	}
	if (stcs->charconv) {
		cmdargv[cmdargc++] = "-C";
		if (stcs->charconv == (EBCCONV | FIXVAR))
			cmdargv[cmdargc++] = "ebcdic,block";
		else if (stcs->charconv & FIXVAR)
			cmdargv[cmdargc++] = "block";
		else
			cmdargv[cmdargc++] = "ebcdic";
	}
	if (stcs->recfm[0]) {
		cmdargv[cmdargc++] = "-F";
		if (strcmp (stcs->recfm, "U,b") == 0)
			cmdargv[cmdargc++] = "U,bin";
		else if (strcmp (stcs->recfm, "U,f") == 0)
			cmdargv[cmdargc++] = "U,f77";
		else if (strcmp (stcs->recfm, "F,-") == 0)
			cmdargv[cmdargc++] = "F,-f77";
		else
			cmdargv[cmdargc++] = stcs->recfm;
	}
	if (stcs->lrecl) {
		cmdargv[cmdargc++] = "-L";
		sprintf (arg_lrecl, "%d", stcs->lrecl);
		cmdargv[cmdargc++] = arg_lrecl;
	}
	if (stcs->nread) {
		samesize = 1;
		for (stcp = stcs+1; stcp < stce; stcp++) {
			if (stcp->nread != stcs->nread) {
				samesize = 0;
				break;
			}
		}
		arg_nread = malloc (6 * (samesize ? 1 : stce - stcs));
		cmdargv[cmdargc++] = "-N";
		sprintf (arg_nread, "%d", stcs->nread);
		if (! samesize)
			for (stcp = stcs+1; stcp < stce; stcp++) {
				sprintf (arg_nread+strlen(arg_nread), ":%d", stcp->nread);
			}
		cmdargv[cmdargc++] = arg_nread;
	}
	if (stcs->size) {
		samesize = 1;
		for (stcp = stcs+1; stcp < stce; stcp++) {
			if (stcp->size != stcs->size) {
				samesize = 0;
				break;
			}
		}
		arg_size = malloc (5 * (samesize ? 1 : stce - stcs));
		cmdargv[cmdargc++] = "-s";
		sprintf (arg_size, "%d", stcs->size);
		if (! samesize)
			for (stcp = stcs+1; stcp < stce; stcp++) {
				sprintf (arg_size+strlen(arg_size), ":%d", stcp->size);
			}
		cmdargv[cmdargc++] = arg_size;
	}

	if (stcs->t_or_d == 't') {
#ifndef TMS
		if (stcs->u1.t.den[0]) {
			cmdargv[cmdargc++] = "-d";
			cmdargv[cmdargc++] = stcs->u1.t.den;
		}
#endif
		if (stcs->u1.t.E_Tflags & SKIPBAD) {
			cmdargv[cmdargc++] = "-E";
			cmdargv[cmdargc++] = "skip";
		}
		if (stcs->u1.t.E_Tflags & KEEPFILE) {
			cmdargv[cmdargc++] = "-E";
			cmdargv[cmdargc++] = "keep";
		}
		if (stcs->u1.t.E_Tflags & IGNOREEOI) {
			cmdargv[cmdargc++] = "-E";
			cmdargv[cmdargc++] = "ignoreeoi";
		}
		if (stcs->u1.t.fid[0])  {
			samefid = 1;
			for (stcp = stcs+1; stcp < stce; stcp++) {
				if (strcmp (stcp->u1.t.fid, stcs->u1.t.fid)) {
					samefid = 0;
					break;
				}
			}
			arg_fid = malloc (18 * (samefid ? 1 : stce - stcs));
			cmdargv[cmdargc++] = "-f";
			strcpy (arg_fid, stcs->u1.t.fid);
			if (! samefid)
				for (stcp = stcs+1; stcp < stce; stcp++) {
					sprintf (arg_fid+strlen(arg_fid), ":%s", stcp->u1.t.fid);
				}
			cmdargv[cmdargc++] = arg_fid;
		}

#ifndef TMS
		cmdargv[cmdargc++] = "-g";
		cmdargv[cmdargc++] = stcs->u1.t.dgn;
#endif
#ifdef TMS
		if (strcmp (stcs->u1.t.lbl, "blp") == 0) {
#endif
			cmdargv[cmdargc++] = "-l";
			cmdargv[cmdargc++] = stcs->u1.t.lbl;
#ifdef TMS
		}
#endif
		if (stcs->u1.t.filstat) {
			sprintf (arg_filstat, "-%c", stcs->u1.t.filstat);
			cmdargv[cmdargc++] = arg_filstat;
		}
		cmdargv[cmdargc++] = "-q";
		strcpy (arg_fseq, stcs->u1.t.fseq);
		if (stce > (stcs+1)) {
			switch (stcs->u1.t.fseq[0]) {
			case 'n':
			case 'u':
				nbtpf = 1;
				for (stcp = stcs+1; stcp < stce; stcp++)
					if (stcp->reqid != (stcp-1)->reqid) nbtpf++;
				if (nbtpf > 1)
					sprintf (arg_fseq+1, "%d", nbtpf);
				break;
			default:
				q = (stce-1)->u1.t.fseq + strlen ((stce-1)->u1.t.fseq) - 1;
				if ((trailing = *q) == '-')
					*q = '\0';
				nbtpf = stce - stcs - 1;
				p = strtok ((stce-1)->u1.t.fseq, ",");
				while (p != NULL) {
					if ((q = strchr (p, '-')) != NULL) {
						*q = '\0';
						n2 = atoi (q + 1);
						n1 = atoi (p);
						*q = '-';
					} else {
						n1 = atoi (p);
						n2 = n1;
					}
					nbtpf += n2 - n1 + 1;
					if ((p = strtok (NULL, ",")) != NULL)
						*(p - 1) = ',';
				}
				fseq_list = (fseq_elem *) calloc (nbtpf, sizeof(fseq_elem));
				nbtpf = 0;
				for (stcp = stcs; stcp < stce-1; stcp++, nbtpf++)
					strcpy ((char *)(fseq_list + nbtpf), stcp->u1.t.fseq);
				p = strtok ((stce-1)->u1.t.fseq, ",");
				while (p != NULL) {
					if ((q = strchr (p, '-')) != NULL) {
						*q = '\0';
						n2 = atoi (q + 1);
						n1 = atoi (p);
						*q = '-';
					} else {
						n1 = atoi (p);
						n2 = n1;
					}
					for (i = n1; i <= n2; i++, nbtpf++)
						sprintf ((char *)(fseq_list + nbtpf), "%d", i);
					p = strtok (NULL, ",");
				}
				if (packfseq (fseq_list, 0, 1, nbtpf, trailing,
				    arg_fseq, sizeof(arg_fseq))) {
					sendrep (rpfd, MSG_ERR, STG21);
					exit (USERR);
				}
			}
		}
		cmdargv[cmdargc++] = arg_fseq;

		if (stcs->u1.t.tapesrvr[0]) {
			cmdargv[cmdargc++] = "-S";
			cmdargv[cmdargc++] = stcs->u1.t.tapesrvr;
		}
		if (stcs->u1.t.E_Tflags & NOTRLCHK)
			cmdargv[cmdargc++] = "-T";
		if (stcs->u1.t.retentd) {
			cmdargv[cmdargc++] = "-t";
			sprintf (arg_retentd, "%d", stcs->u1.t.retentd);
			cmdargv[cmdargc++] = arg_retentd;
		}
		cmdargv[cmdargc++] = "-V";
		strcpy (arg_vid, stcs->u1.t.vid[0]);
		for (i = 1; i < MAXVSN; i++)
			if (*stcs->u1.t.vid[i])
				sprintf (arg_vid+strlen(arg_vid), ":%s", stcs->u1.t.vid[i]);
			else
				break;
		cmdargv[cmdargc++] = arg_vid;
#ifndef TMS
		cmdargv[cmdargc++] = "-v";
		strcpy (arg_vsn, stcs->u1.t.vsn[0]);
		for (i = 1; i < MAXVSN; i++)
			if (*stcs->u1.t.vsn[i])
				sprintf (arg_vsn+strlen(arg_vsn), ":%s", stcs->u1.t.vsn[i]);
			else
				break;
		cmdargv[cmdargc++] = arg_vsn;
#endif
	}

	cmdargv[cmdargc++] = "-Z";
	sprintf (arg_Z, "%d.%d@%s", reqid, key, hostname);
	cmdargv[cmdargc++] = arg_Z;

	if (stcs->t_or_d == 't') {
		if (! Aflag) {
			for (stcp = stcs; stcp < stce; stcp++)
				cmdargv[cmdargc++] = stcp->ipath;
		} else {
			for (stcp = stcs; stcp < stce; stcp++)
				cmdargv[cmdargc++] = ".";
		}
	}
	cmdargv[cmdargc] = NULL;

	/* creating pipe to get stderr from tpread/tpwrite */

	if (pipe (pfd) < 0) {
		sendrep (rpfd, MSG_ERR, STG02, "", "pipe", sys_errlist[errno]);
		exit (SYERR);
	}
	if (fcntl (pfd[0], F_SETFL, O_NDELAY) < 0 ) {
		sendrep (rpfd, MSG_ERR, STG02, "", "fcntl", sys_errlist[errno]);
		exit (SYERR);
	}

	/* fork and exec tpread/tpwrite process */

	pid = fork ();
	if (pid < 0) {
		sendrep (rpfd, MSG_ERR, STG02, "", "fork", sys_errlist[errno]);
		exit (SYERR);
	} else if (pid == 0) {	/* we are in the child */
		stglogit (func, "execing %s, pid=%d\n", cmdname, getpid());
		for (i = 0; i < maxfds; i++)
			if (i != pfd[1]) close (i);
		dup2 (pfd[1], 2);
		(void) umask (stcs->mask);
		setgid (stcs->gid);
		setuid (stcs->uid);
		sprintf (progfullpath, "%s/%s", BIN, cmdname);

		execvp (progfullpath, cmdargv);
		stglogit (func, STG02, cmdname, "execvp", sys_errlist[errno]);
		exit (SYERR);
	}
	FD_ZERO (&readmask);
	FD_ZERO (&readfd);
	FD_SET (pfd[0], &readmask);
	while (1) {
		if (FD_ISSET (pfd[0], &readfd)) {
			while ((l = read (pfd[0], buf, sizeof(buf)-1)) > 0) {
				buf[l] = '\0';
				sendrep (rpfd, RTCOPY_OUT, "%s", buf);
			}
			FD_CLR (pfd[0], &readfd);
                }
		if (ovl_status >=0) break;
                memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = CHECKI;	/* must set each time for linux */
		timeval.tv_usec = 0;
		if (select (maxfds, &readfd, (fd_set *)0, (fd_set *)0, &timeval) < 0) {
			FD_ZERO (&readfd);
		}
	}
	close (pfd[0]);
	exit (ovl_status);
}

void checkovlstatus(pid, status)
int pid;
int status;
{
	stglogit (func, "process %d exiting with status %x\n", pid, status & 0xFFFF);
	ovl_status = (status & 0xFF) ? SYERR : ((status >> 8) & 0xFF);
}

filecopy(stcp, key, hostname)
struct stgcat_entry *stcp;
int key;
char *hostname;
{
	char buf[256];
	int c;
	char command[2*(CA_MAXHOSTNAMELEN + 1 + MAXPATH) + CA_MAXHOSTNAMELEN + 1 + 196];
	char *filename;
	char *host;
	char *p, *q;
	RFILE *rf;
	char savebuf[256];
	int saveflag = 0;

	(void) rfio_parseln (stcp->ipath, &host, &filename, NORDLINKS);

	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);
	if (host)
		sprintf (command, "%s:%s/cpdskdsk", host, BIN);
	else 
		sprintf (command, "%s/cpdskdsk", BIN);
	sprintf (command+strlen(command), " -Z %d.%d@%s", reqid, key, hostname);

	if ((stcp->t_or_d == 'd') && (stcp->u1.d.Xparm[0]))
		sprintf (command+strlen(command), " -X %s", stcp->u1.d.Xparm);

	/* Writing file */
	if ((stcp->status == STAGEWRT) || (stcp->status == STAGEPUT)) {
		sprintf (command+strlen(command), " -s %d",(int) stcp->actual_size);
		sprintf (command+strlen(command), " %s", stcp->ipath);
		if (stcp->t_or_d == 'm')
			sprintf (command+strlen(command), " %s", stcp->u1.m.xfile);
		else
			sprintf (command+strlen(command), " %s", stcp->u1.d.xfile);
	} else {
		/* Reading file */
		if (stcp->size)
			sprintf (command+strlen(command), " -s %d",
				stcp->size * 1024 * 1024);
		if (stcp->t_or_d == 'm')
			sprintf (command+strlen(command), " %s", stcp->u1.m.xfile);
		else
			sprintf (command+strlen(command), " %s", stcp->u1.d.xfile);	    
		sprintf (command+strlen(command), " %s", stcp->ipath);
	}
	stglogit (func, "execing command : %s\n", command);
	setgid (stcp->gid);
	setuid (stcp->uid);
	rf = rfio_popen (command, "r");
	if (rf == NULL) {
		stglogit (func, "%s : %s\n", command, rfio_serror());
		return (SYERR);
	}

	while ((c = rfio_pread (buf, 1, sizeof(buf)-1, rf)) > 0) {
		buf[c] = '\0';
		sendrep (rpfd, RTCOPY_OUT, "%s", buf);
	}

	c = rfio_pclose (rf);
	return (c);
}

void stagekilled()
{
	int c;

	if (pid) {
		stglogit (func, "killing process %d\n", pid);
		c = kill (pid, SIGINT);
	}
	exit (REQKILD);
}

#if defined(ultrix) || (defined(sun) && !defined(SOLARIS))
void wait4child()
{
	int pid;
	union wait status;

	while ((pid = wait3 (&status, WNOHANG, (struct rusage *) 0)) > 0)
		checkovlstatus (pid, status.w_status);
}
#else
void wait4child()
{
	int pid;
	int status;

#if defined(_IBMR2) || defined(SOLARIS) || defined(IRIX5) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	while ((pid = waitpid (-1, &status, WNOHANG)) > 0)
		checkovlstatus (pid, status);
#else
	pid = wait (&status);
	checkovlstatus (pid, status);
#if _IBMESA
	signal (SIGCHLD, wait4child);
#else
	signal (SIGCLD, wait4child);
#endif
#endif
}
#endif
