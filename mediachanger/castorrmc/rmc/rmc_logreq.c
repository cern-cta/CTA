/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2001-2022 CERN
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

#include "rmc_constants.h"
#include "json_logger.h"
#include "rmc_logreq.h"

#include <string.h>

/*	rmc_logreq - log a request */

/*	Split the message into lines so they don't exceed LOGBUFSZ-1 characters
 *	A backslash is appended to a line to be continued
 *	A continuation line is prefixed by '+ '
 */
// void rmc_logreq(enum LogLevel lvl, const char *const func, const char *const operation, const char *const status, const char *const msg) {
//   char fieldsbuf[RMC_PRTBUFSZ];
//   int off = snprintf(fieldsbuf, sizeof fieldsbuf, ",\"operation\":\"%s\",\"status\":\"%s\"", operation, status);
//   JSON_LOG_CONTEXT(lvl, fieldsbuf, func, msg);
// }
