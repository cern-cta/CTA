/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: findpgrp.c,v $ $Revision: 1.2 $ $Date: 1999/09/20 11:31:31 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	findpgrp - get process group (replaces getpgrp broken by C-shell) */

#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#ifndef linux
#if hpux
#include <sys/param.h>
#include <sys/pstat.h>
#else
#if defined(SOLARIS) || defined(sgi) || (defined(__osf__) && defined(__alpha))
#if defined(__osf__) && defined(__alpha)
#include <sys/ioctl.h>
#endif
#include <sys/procfs.h>
#else
#if _AIX && _IBMR2
#include <nlist.h>
#include <sys/proc.h>
#include <sys/sysconfig.h>
#include <sys/var.h>
#endif
#endif
#endif
#endif
#include "Ctape_api.h"
#include "Ctape.h"
static char func[16];
extern char *sys_errlist[];
findpgrp()
{
	int pgrp, pid;
	uid_t uid;
#ifndef linux
#if hpux
	struct pst_status stproc;
#else
	int fdkmem;
#if defined(SOLARIS) || defined(sgi) || (defined(__osf__) && defined(__alpha))
	struct prpsinfo prpsinfo;
	char name[12];
#else
#if _AIX && _IBMR2
	struct proc *findproc();
	long lastprocaddr;
	static struct nlist nl[3];
	struct proc *p;
	struct proc *proctab;
	long proctabaddr;
	int proctabsiz;
	struct var var;
#endif
#endif
#endif
#endif
	strcpy (func, "findpgrp");
	pid = getpid();
	pgrp = pid;
	uid = getuid();

#if defined(linux)
	if ((pgrp = getsid (0)) < 0) {
		Ctape_errmsg (func, TP002, "getsid", sys_errlist[errno]);
		return (-1);
	}
#else
#if hpux
	while (pid != 1) {
		if (pstat_getproc (&stproc, sizeof(struct pst_status), 0, pid) < 0) {
			Ctape_errmsg (func, TP002, "pstat_getproc", sys_errlist[errno]);
			return (-1);
		}
		if (stproc.pst_uid != uid) break;
		pgrp = stproc.pst_pid;
		pid = stproc.pst_ppid;
	}
#else
#if defined(SOLARIS) || defined(sgi) || (defined(__osf__) && defined(__alpha))
	sprintf (name, "/proc/%.5d", pid);
	fdkmem = open (name, 0);
	if (fdkmem < 0) {
		Ctape_errmsg (func, TP002, name, "open", errno);
		return (-1);
	}
	if (ioctl (fdkmem, PIOCPSINFO, &prpsinfo) < 0) {
		Ctape_errmsg (func, TP002, name, "ioctl", errno);
		close (fdkmem);
		return (-1);
	}
	pgrp = prpsinfo.pr_sid;
	close (fdkmem);
#else
#if _AIX && _IBMR2
	nl[0].n_name = "proc";
	knlist (nl, 1, sizeof(struct nlist));
	proctabaddr = nl[0].n_value;
	if (sysconfig (SYS_GETPARMS, &var, sizeof(var)) < 0) {
		Ctape_errmsg (func, TP002, "sysconfig", sys_errlist[errno]);
		return (-1);
	}
	lastprocaddr = (long) var.ve_proc;
	fdkmem = open ("/dev/kmem", 0);
	if (fdkmem < 0) {
		Ctape_errmsg (func, "TP002 - /dev/kmem : open error : %s\n",
		    sys_errlist[errno]);
		return (-1);
	}
	proctabsiz = lastprocaddr - proctabaddr;
	proctab = (struct proc *) malloc (proctabsiz);
	if (proctab == 0) {
		Ctape_errmsg (func, TP005);
		close (fdkmem);
		return (-1);
	}
	if (seek_and_read (fdkmem, proctabaddr, proctab, proctabsiz) < 0) {
		close (fdkmem);
		free (proctab);
		return (-1);
	}
	close (fdkmem);

	while ((p = findproc (proctab, pid))->p_uid == uid) {
		if (p->p_ppid == 1) break;
		pgrp = p->p_pid;
		pid = p->p_ppid;
	}
	free (proctab);
#endif
#endif
#endif
#endif
	return (pgrp);
}
#if _AIX && _IBMR2
struct proc *
findproc(proctab, pid)
struct proc *proctab;
int pid;
{
	struct proc *p;
	p = proctab;
	while (p->p_pid != pid) p++;
	return (p);
}
#define TWO_GIG (0x80000000)
static int
seek_and_read(fd, offset, data, data_len)
int fd;
long offset;
void *data;
int data_len;
{
	int rc;

	if (offset >= TWO_GIG) {
		if (lseek (fd, offset - TWO_GIG, 0) < 0) {
			Ctape_errmsg (func, "TP002 - /dev/kmem : lseek error : %s\n",
			    sys_errlist[errno]);
			return (-1);
		}
		if (readx (fd, data, data_len ,1) < 0) {
			Ctape_errmsg (func, "TP002 - /dev/kmem : readx error : %s\n",
			    sys_errlist[errno]);
			return (-1);
		}
	} else {
		if (lseek (fd, offset, 0) < 0) {
			Ctape_errmsg (func, "TP002 - /dev/kmem : lseek error : %s\n",
			    sys_errlist[errno]);
			return (-1);
		}
		if (readx (fd, data, data_len ,0) < 0) {
			Ctape_errmsg (func, "TP002 - /dev/kmem : readx error : %s\n",
			    sys_errlist[errno]);
			return (-1);
		}
	}
	return (0);
}
#endif
