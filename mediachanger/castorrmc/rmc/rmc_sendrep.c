/*
 * Copyright (C) 2001-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include "h/marshall.h"
#include "h/net.h"
#include "h/rmc_constants.h"
#include "h/rmc_logit.h"
#include "h/rmc_sendrep.h"
#include "h/tplogger_api.h"
#include <unistd.h>

int rmc_sendrep(const int rpfd, const int rep_type, ...)
{
	va_list args;
	char func[16];
	char *msg;
	int n;
	char prtbuf[RMC_PRTBUFSZ];
	char *rbp;
	int rc;
	char repbuf[RMC_REPBUFSZ];
	int repsize;

	strncpy (func, "rmc_sendrep", sizeof(func));
	func[sizeof(func) - 1] = '\0';
	rbp = repbuf;
	marshall_LONG (rbp, RMC_MAGIC);
	va_start (args, rep_type);
	marshall_LONG (rbp, rep_type);
	switch (rep_type) {
	case MSG_ERR:
		msg = va_arg (args, char *);
		vsprintf (prtbuf, msg, args);
		marshall_LONG (rbp, strlen (prtbuf) + 1);
		marshall_STRING (rbp, prtbuf);
		rmc_logit (func, "%s", prtbuf);
                tl_rmcdaemon.tl_log( &tl_rmcdaemon, 103, 2,
                                     "func"   , TL_MSG_PARAM_STR, func,
                                     "Message", TL_MSG_PARAM_STR, prtbuf );
		break;
	case MSG_DATA:
		n = va_arg (args, int);
		marshall_LONG (rbp, n);
		msg = va_arg (args, char *);
		memcpy (rbp, msg, n);	/* marshalling already done */
		rbp += n;
		break;
	case RMC_RC:
		rc = va_arg (args, int);
		marshall_LONG (rbp, rc);
		break;
	}
	va_end (args);
	repsize = rbp - repbuf;
	if (netwrite (rpfd, repbuf, repsize) != repsize) {
		rmc_logit (func, RMC02, "send", neterror());
                tl_rmcdaemon.tl_log( &tl_rmcdaemon, 2, 3,
                                     "func" , TL_MSG_PARAM_STR, func,
                                     "On"   , TL_MSG_PARAM_STR, "send",
                                     "Error", TL_MSG_PARAM_STR, neterror() );
		if (rep_type == RMC_RC)
			close (rpfd);
		return (-1);
	}
	if (rep_type == RMC_RC)
		close (rpfd);
	return (0);
}
