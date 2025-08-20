/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2003-2025 CERN
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

#pragma once

#include <stddef.h>

/**
 * This function wraps the XSI compliant version of strerror_r() and therefore
 * writes the string representation of the specified error number to the
 * specified buffer.
 *
 * This function should be compiled using a C++ compiler and not a C compiler.
 *
 * C++ compilers are better at spotting whether the GNU version or the
 * XSI complicant version of sterror_() is being used. This is because the
 * difference between the two versions is their return types.  The GNU
 * version returns a 'char *' whereas the XSI compliant version returns an
 * 'int'.  A C compiler may allow the strerror_r() function to return a
 * 'char *' and have that 'char *' assigned to an 'int'.  A C++ compiler
 * usually gives an error if this is tried.
 *
 * @param errnum The error number.
 * @param buf The buffer.
 * @param buflen The length of the buffer.
 * @return 0 on success and -1 on error.  If -1 is returned then errno is set
 * to either EINVAL to indicate the error number is invalid, or to ERANGE to
 * indicate the supplied error buffer is not large enough.
 */
int strerror_r_wrapper(int errnum, char* buf, size_t buflen);
