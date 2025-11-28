/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1990-2025 CERN
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

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include "serrno.hpp"  //!< special error numbers and codes
#include "Cglobals.hpp"
#include "strerror_r_wrapper.hpp"

const char* sys_serrlist[SEMAXERR - SEBASEOFF + 2] = {"Error 0",
                                                      "Host not known",
                                                      "Service unknown",
                                                      "Not a remote file",
                                                      "Timed out",
                                                      "Unsupported FORTRAN format",
                                                      "Unknown FORTRAN option",
                                                      "Incompatible FORTRAN options",
                                                      "File name too long",
                                                      "Can't open configuration file",
                                                      "Version ID mismatch",
                                                      "User buffer too small",
                                                      "Invalid reply number",
                                                      "User message too long",
                                                      "Entry not found",
                                                      "Internal error",
                                                      "Connection closed by remote end",
                                                      "Can't find interface name",
                                                      "Communication error",
                                                      "Can't open mapping database",
                                                      "No user mapping",
                                                      "Retry count exhausted",
                                                      "Operation not supported",
                                                      "Resource temporarily unavailable",
                                                      "Operation now in progress",
                                                      "Cthread initialization error",
                                                      "Thread interface call error",
                                                      "System error",
                                                      "deprecated error",
                                                      "deprecated error",
                                                      "deprecated error",
                                                      "deprecated error",
                                                      "requestor is not administrator",
                                                      "User unknown",
                                                      "Duplicate key value",
                                                      "Entry already exists",
                                                      "Group unknown",
                                                      "Bad checksum",
                                                      "This service class is not available for this host",
                                                      "Got SQL exception from database",
                                                      "Too many symbolic links",
                                                      "No port in range",
                                                      "No value",
                                                      "Invalid configuration",
                                                      "Failed to execute Python",
                                                      "Missing operand",
                                                      "Mismatch",
                                                      "Request failed",
                                                      "Invalid number of arguments",
                                                      "Already initialized",
                                                      "Command line not parsed",
                                                      "Accept connection was interrupted",
                                                      "Failed to allocate memory",
                                                      "Not an owner",
                                                      "BAD ERROR NUMBER"};

/*
 * Package specific error messages (don't forget to update h/serrno.h)
 */

/*
 *------------------------------------------------------------------------
 * RMC (Remote SCSI Media Changer server) specific error messages
 *------------------------------------------------------------------------
 */
const char* sys_rmerrlist[ERMMAXERR - ERMBASEOFF + 2] = {"Error 0",
                                                         "Remote SCSI media changer server not active",
                                                         "Remote SCSI media changer error",
                                                         "Remote SCSI media changer unrec. error",
                                                         "Remote SCSI media changer error (slow retry)",
                                                         "Remote SCSI media changer error (fast retry)",
                                                         "Remote SCSI media changer error (demount force)",
                                                         "Remote SCSI media changer error (drive down)",
                                                         "Remote SCSI media changer error (ops message)",
                                                         "Remote SCSI media changer error (ops message + retry)",
                                                         "Remote SCSI media changer error (ops message + wait)",
                                                         "Remote SCSI media changer error (unload + demount)",
                                                         "BAD ERROR NUMBER"};

int sstrerror_r(const int n, char* const buf, const size_t buflen) {
  const char* tmpstr;
  char strerror_r_buf[100];

  if (buf == nullptr || buflen <= 0) {
    errno = EINVAL;
    serrno = EINVAL;
    return -1;
  }

  memset(buf, '\0', buflen);
  tmpstr = nullptr;

  if ((n > SEBASEOFF) && (n <= SEMAXERR)) {
    // COMMON error messages
    tmpstr = sys_serrlist[n - SEBASEOFF];
  } else if ((n > ERMBASEOFF) && (n <= ERMMAXERR)) {
    // RMC (Remote SCSI media changer server) specific error messages
    tmpstr = sys_rmerrlist[n - ERMBASEOFF];
  } else if (n > 0) {
    // SYSTEM error messages
    const int strerror_r_wrapper_rc = strerror_r_wrapper(n, strerror_r_buf, sizeof(strerror_r_buf));
    if (strerror_r_wrapper_rc) {
      tmpstr = "Unknown error because call to strerror_r_wrapper() failed";
    } else {
      tmpstr = strerror_r_buf;
    }
  }

  if (tmpstr != nullptr) {
    strncpy(buf, tmpstr, buflen);
    buf[buflen - 1] = '\0';
  } else {
    // Unknown error message
    snprintf(buf, buflen, "%*s: %10d", (int) buflen - 14, sys_serrlist[SEMAXERR + 1 - SEBASEOFF], n);
  }
  return 0;
}

void sperror(char* msg) {
  char buf[80];
  if (serrno) {
    if (sstrerror_r(serrno, buf, sizeof(buf))) {
      /* sstrerror_r() failed so just print msg */
      fprintf(stderr, "%s\n", msg);
    } else {
      /* sstrerror_r() succeeded so print both msg and buf */
      fprintf(stderr, "%s: %s\n", msg, buf);
    }
  } else {
    perror(msg);
  }
}

static int sstrerror_key = -1;

const char* sstrerror(const int n) {
  void* buf = nullptr;
  int buflen = 80;

  if (0 > Cglobals_get(&sstrerror_key, &buf, buflen)) {
    return "Unknown error"
           ": No thread specific memory for error string"
           ": Cglobals_get() failed";
  }

  if (sstrerror_r(n, (char*) buf, buflen)) {
    return "Unknown error"
           ": sstrerror_r() failed";
  } else {
    return reinterpret_cast<char*>(buf);
  }
}
