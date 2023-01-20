/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2000-2022 CERN
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

/*
 * marshall.c - wrappers on top of marshall macros
 */

#include <string.h>
#include "mediachanger/castorrmc/h/marshall.h"
#include "mediachanger/castorrmc/h/osdep.h"

int
_unmarshall_STRINGN(char **ptr,
                        char *str,
                        int n)
{
	char *p;

	(void) strncpy (str, *ptr, n);
	if ((p = memchr (str, 0, n)) != NULL) {
		*ptr += (p - str + 1);
		return (0);
	}
	*(str + n - 1) = '\0';
	*ptr += strlen(*ptr) + 1;
	return (-1);
}
