/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2007-2022 CERN
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

#include "mediachanger/castorrmc/h/Csnprintf.h"

/* Hide the snprintf and al. call v.s. different OS. */
/* Sometimes a different name, sometimes do not exist */

int Csnprintf(char *str, size_t size, const char *format, ...) {
	int rc;
	va_list args;

	va_start (args, format);
	/* Note: we cannot call sprintf again, because a va_list is a real */
	/* parameter on its own - it cannot be changed to a real list of */
	/* parameters on the stack without being not portable */
	rc = Cvsnprintf(str,size,format,args);
	va_end (args);
	return(rc);
}

int Cvsnprintf(char *str, size_t size, const char *format, va_list args)
{
	return(vsnprintf(str, size, format, args));
}
