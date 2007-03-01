/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: checkjobdied.c,v $ $Revision: 1.7 $ $Date: 2007/03/01 16:41:37 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	checkjobdied - returns the list of jobs that have died */

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#ifndef hpux
#if defined(SOLARIS) || defined(sgi) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(AIX51)
#include <errno.h>
#include <sys/stat.h>
#else
#if _AIX && _IBMR2
#include <nlist.h>
#include <sys/proc.h>
#include <sys/sysconfig.h>
#include <sys/var.h>
#endif
#endif
#endif
#include "Ctape.h"
#include "serrno.h"
static char func[16];

int checkjobdied(jobs)
int jobs[];
{
	int i, j, k;
#ifndef hpux
#if defined(SOLARIS) || defined(sgi) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(AIX51)
	char name[12];
	struct stat st;
#else
#if _AIX && _IBMR2
	int fdkmem;
	long lastprocaddr;
	struct nlist nl[3];
	int nproc;
	struct proc *p;
	struct proc *proctab;
	long proctabaddr;
	int proctabsiz;
	struct var var;
#endif
#endif
#endif
	ENTRY (checkjobdied);
#if hpux
	for (i = 0, k = 0; jobs[i]; i++) {
		if (kill(jobs[i], 0) != 0)
			jobs[k++] = jobs[i];		/* job has died */
	}
#else
#if defined(linux)
	for (i = 0, k = 0; jobs[i]; i++) {
		sprintf (name, "/proc/%d", jobs[i]);
		j = stat (name, &st);
		if (j < 0 && errno == ENOENT) jobs[k++] = jobs[i];	/* job has died */
	}
#else
#if defined(SOLARIS) || defined(sgi) || (defined(__osf__) && defined(__alpha)) || defined(AIX51)
	for (i = 0, k = 0; jobs[i]; i++) {
		sprintf (name, "/proc/%.5d", jobs[i]);
		j = stat (name, &st);
		if (j < 0 && errno == ENOENT) jobs[k++] = jobs[i];	/* job has died */
	}
#else
#if _AIX && _IBMR2
	nl[0].n_name = "proc";
	knlist (nl, 1, sizeof(struct nlist));
	proctabaddr = nl[0].n_value;
	if (sysconfig (SYS_GETPARMS, &var, sizeof(var)) < 0) {
		serrno = errno;
		tplogit (func, TP002, "sysconfig", strerror(errno));
		RETURN (-1);
	}
	lastprocaddr = (long) var.ve_proc;
	fdkmem = open ("/dev/kmem", 0);
	if (fdkmem < 0) {
		serrno = errno;
		tplogit (func, "TP002 - /dev/kmem : open error : %s\n", strerror(errno));
		RETURN (-1);
	}
	proctabsiz = lastprocaddr - proctabaddr;
	proctab = (struct proc *) malloc (proctabsiz);
	if (proctab == 0) {
		serrno = errno;
		tplogit (func, TP005);
		close (fdkmem);
		RETURN (-1);
	}
	if (seek_and_read (fdkmem, proctabaddr, proctab, proctabsiz) < 0) {
		serrno = errno;
		close (fdkmem);
		free (proctab);
		RETURN (-1);
	}
	close (fdkmem);
	nproc = proctabsiz / sizeof(struct proc);

	for (i = 0, k = 0; jobs[i]; i++) {
		for (j = 0, p = proctab; j < nproc; j++, p++)
			if (p->p_pid == jobs[i] &&
			    p->p_stat != 0) break;		/* job still alive */
		if (j == nproc) jobs[k++] = jobs[i];		/* job has died */
	}
	free (proctab);
#endif
#endif
#endif
#endif
	RETURN (k);
}

#if _AIX && _IBMR2
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
			tplogit (func, "TP002 - /dev/kmem : lseek error : %s\n",
			    strerror(errno));
			return (-1);
		}
		if (readx (fd, data, data_len ,1) < 0) {
			tplogit (func, "TP002 - /dev/kmem : readx error : %s\n",
			    strerror(errno));
			return (-1);
		}
	} else {
		if (lseek (fd, offset, 0) < 0) {
			tplogit (func, "TP002 - /dev/kmem : lseek error : %s\n",
			    strerror(errno));
			return (-1);
		}
		if (readx (fd, data, data_len ,0) < 0) {
			tplogit (func, "TP002 - /dev/kmem : readx error : %s\n",
			    strerror(errno));
			return (-1);
		}
	}
	return (0);
}
#endif
