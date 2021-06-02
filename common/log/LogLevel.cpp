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

#include "common/exception/Exception.hpp"
#include "common/log/Constants.hpp"

namespace cta {
namespace log {

//------------------------------------------------------------------------------
// toLogLevel
//------------------------------------------------------------------------------
int toLogLevel(const std::string &s) {
  if(s == "EMERG")   return EMERG;
  if(s == "ALERT")   return ALERT;
  if(s == "CRIT")    return CRIT;
  if(s == "ERR")     return ERR;
  if(s == "WARNING") return WARNING;
  if(s == "NOTICE")  return NOTICE;
  if(s == "INFO")    return INFO;
  if(s == "DEBUG")   return DEBUG;

  // It is a convention of CTA to use syslog level of LOG_NOTICE to label
  // user errors.
  if(s == "USERERR") return NOTICE;

  throw exception::Exception(s + " is not a valid log level");
}

} // namespace log
} // namespace cta
