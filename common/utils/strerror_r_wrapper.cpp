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
#include "common/utils/strerror_r_wrapper.hpp"

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
   */
  return strerror_r(errnum, buf, buflen);
}
