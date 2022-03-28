/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2001-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#include <errno.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include "marshall.h"
#include "net.h"
#include "rmc_constants.h"
#include "rmc_logit.h"
#include "rmc_sendrep.h"
#include <unistd.h>

static const char *rep_type_to_str(const int rep_type) {
	switch(rep_type) {
	case MSG_ERR : return "MSG_ERR";
	case MSG_DATA: return "MSG_DATA";
	case RMC_RC  : return "RMC_RC";
	default      : return "UNKNOWN";
	}
}

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
                const char *const neterror_str = neterror();
		rmc_logit (func, RMC02, "send", neterror_str);
		rmc_logit (func, "Call to netwrite() failed"
			": rep_type=%s neterror=%s\n",
			rep_type_to_str(rep_type), neterror_str);
		if (rep_type == RMC_RC)
			close (rpfd);
		return (-1);
	}
	if (rep_type == RMC_RC)
		close (rpfd);
	return (0);
}
