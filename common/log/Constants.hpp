/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include <string>

namespace cta {
namespace log {

const int EMERG   = 0; // system is unusable
const int ALERT   = 1; // action must be taken immediately
const int CRIT    = 2; // critical conditions
const int ERR     = 3; // error conditions
const int WARNING = 4; // warning conditions
const int NOTICE  = 5; // normal but signification condition
const int INFO    = 6; // informational
const int DEBUG   = 7; // debug-level messages

/**
 * It is a convention of CTA to use syslog level of LOG_NOTICE to label
 * user errors.
 */
const int USERERR = NOTICE;

} // namespace log
} // namespace cta
