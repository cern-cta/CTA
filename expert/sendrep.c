/*
 * 
 * Copyright (C) 2004 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: sendrep.c,v $ $Revision: 1.1 $ $Date: 2004/06/30 16:18:36 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <sys/types.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <stdarg.h>
#include "marshall.h"
#include "net.h"
#include "expert.h"
#include "serrno.h"

int sendrep(int rpfd, int rep_type, ...)
{
	va_list ap;
	char func[16];
	int n;
	char *rbp;
	int req_type;
	char repbuf[EXP_REPBUFSZ+12];
	int repsize;

	rbp = repbuf;

	va_start (ap, rep_type);

	switch (rep_type) {
	case EXP_RP_STATUS:
	        marshall_LONG (rbp, EXP_MAGIC);
	        marshall_LONG (rbp, rep_type);
		n = va_arg (ap, int);
		marshall_LONG (rbp, n);
		n = va_arg (ap, int);
		marshall_LONG (rbp, n);
		break;
	case EXP_RP_ERRSTRING:
	        marshall_STRING(rbp, EXP_ERRSTRING);
		break;
	default:
	        marshall_LONG (rbp, EXP_MAGIC);
	        marshall_LONG (rbp, EXP_RP_STATUS);
		marshall_LONG (rbp, EXP_ST_ERROR);
		marshall_LONG (rbp, SEINTERNAL);
		break;
	}
	va_end (ap);
	repsize = rbp - repbuf;
	if (netwrite (rpfd, repbuf, repsize) != repsize) {
	  	explogit (func, EXP02, "send", neterror());
		if (rep_type == EXP_RP_ERRSTRING)
			netclose (rpfd);
		return (-1);
	}
	return (0);
}
