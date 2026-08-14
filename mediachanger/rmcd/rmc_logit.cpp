/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rmc_logit.hpp"

#include "rmc_constants.hpp"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

std::string rmcFormatLogMessage(const char* const msg, ...) {
  const int savedErrno = errno;
  va_list args;
  char prtbuf[RMC_PRTBUFSZ];
  va_start(args, msg);
  vsnprintf(prtbuf, sizeof(prtbuf), msg, args);
  va_end(args);
  errno = savedErrno;
  std::string formattedMessage = prtbuf;
  if (formattedMessage.ends_with('\n')) {
    formattedMessage.pop_back();
  }
  return formattedMessage;
}
