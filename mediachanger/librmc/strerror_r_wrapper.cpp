/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "strerror_r_wrapper.hpp"

/*
 * Undefine _GNU_SOURCE and define _XOPEN_SOURCE as being 600 so that the
 * XSI compliant version of strerror_r() will be used
 */
#undef _GNU_SOURCE
#define _XOPEN_SOURCE 600

#include <string.h>

/*******************************************************************************
 * strerror_r_wrapper
 ******************************************************************************/
int strerror_r_wrapper(int errnum, char* buf, size_t buflen) {
  /* This function should be compiled using a C++ compiler and not a C compiler.
   *
   * C++ compilers are better at spotting whether the GNU version or the
   * XSI complicant version of sterror_() is being used. This is because the
   * difference between the two versions is their return types.  The GNU
   * version returns a 'char *' whereas the XSI compliant version returns an
   * 'int'.  A C compiler may allow the strerror_r() function to return a
   * 'char *' and have that 'char *' assigned to an 'int'.  A C++ compiler
   * usually gives an error if this is tried.
   */
  return strerror_r(errnum, buf, buflen);
}
