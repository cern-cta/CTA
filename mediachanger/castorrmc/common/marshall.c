/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2000-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * marshall.c - wrappers on top of marshall macros
 */

#include <string.h>
#include "marshall.h"
#include "osdep.h"

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
