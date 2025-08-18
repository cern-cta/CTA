/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "strerror_r_wrapper.h"

#if defined(__linux__)
/*
 * Undefine _GNU_SOURCE and define _XOPEN_SOURCE as being 600 so that the
 * XSI compliant version of strerror_r() will be used
 */
#undef _GNU_SOURCE
#define _XOPEN_SOURCE 600
#endif

#include <string.h>

/*******************************************************************************
 * strerror_r_wrapper
 ******************************************************************************/
extern "C" int strerror_r_wrapper(int errnum, char *buf, size_t buflen) {
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
