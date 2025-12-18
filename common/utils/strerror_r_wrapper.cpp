/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
