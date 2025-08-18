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

#include "rmc_constants.h"
#include "rmc_logit.h"
#include "rmc_logreq.h"

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
	char savechrs2[2] = { '\0', '\0' };

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
