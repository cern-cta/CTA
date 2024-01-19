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

#pragma once

#include <string>

namespace cta::log {

const int EMERG   = 0; // system is unusable
const int ALERT   = 1; // action must be taken immediately
const int CRIT    = 2; // critical conditions
const int ERR     = 3; // error conditions
const int WARNING = 4; // warning conditions
const int NOTICE  = 5; // normal but signification condition
const int INFO    = 6; // informational
const int DEBUG   = 7; // debug-level messages
const int JSON    = 8; ///json level
/**
 * It is a convention of CTA to use syslog level of LOG_NOTICE to label
 * user errors.
 */
const int USERERR = NOTICE;

} // namespace cta::log
