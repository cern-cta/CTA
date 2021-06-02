/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
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

#pragma once

#include <stddef.h>

/* The following EXTERN_C marco has been intentionally copied from          */
/* h/osdep.h instead of just including h/osdep.h.  The reason for this is   */
/* this header file must include the minimum number of header files because */
/* the implementation file common/strerror_r_wrapper.cpp will undefine      */
/* _GNU_SOURCE and define _XOPEN_SOURCE as being 600.                       */
/*                                                                          */
/* Macros for externalization (UNIX) (J.-D.Durand)                          */
#if defined(__cplusplus)
#define EXTERN_C extern "C"
#else
#define EXTERN_C extern
#endif

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
EXTERN_C int strerror_r_wrapper(int errnum, char *buf, size_t buflen);
