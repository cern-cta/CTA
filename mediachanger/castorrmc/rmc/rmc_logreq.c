/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#include "h/rmc_constants.h"
#include "h/rmc_logit.h"
#include "h/rmc_logreq.h"
#include "h/tplogger_api.h"

#include <string.h>
 
/*	rmc_logreq - log a request */

/*	Split the message into lines so they don't exceed LOGBUFSZ-1 characters
 *	A backslash is appended to a line to be continued
 *	A continuation line is prefixed by '+ '
 */
void rmc_logreq(const char *const func, char *const logbuf) {
	int n1, n2;
	char *p;
	char savechrs1[2];
	char savechrs2[2];

	n1 = RMC_LOGBUFSZ - strlen (func) - 36;
	n2 = strlen (logbuf);
	p = logbuf;
	while (n2 > n1) {
		savechrs1[0] = *(p + n1);
		savechrs1[1] = *(p + n1 + 1);
		*(p + n1) = '\\';
		*(p + n1 + 1) = '\0';
		rmc_logit (func, RMC98, p);
		if (p != logbuf) {
			*p = savechrs2[0];
			*(p + 1) = savechrs2[1];
		}
		p += n1 - 2;
		savechrs2[0] = *p;
		savechrs2[1] = *(p + 1);
		*p = '+';
		*(p + 1) = ' ';
		*(p + 2) = savechrs1[0];
		*(p + 3) = savechrs1[1];
		n2 -= n1;
	}
	rmc_logit (func, RMC98, p);
	if (p != logbuf) {
		*p = savechrs2[0];
		*(p + 1) = savechrs2[1];
	}
}
