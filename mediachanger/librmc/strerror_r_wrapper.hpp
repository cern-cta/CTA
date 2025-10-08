/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
