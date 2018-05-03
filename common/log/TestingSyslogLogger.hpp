/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/log/SyslogLogger.hpp"

namespace cta {
namespace log {

/**
 * Class used to facilitate unit testing by making public one or more of the
 * protected members of its super class.
 */
class TestingSyslogLogger: public SyslogLogger {
public:

  /**
   * Constructor
   *
   * @param hostName The name of the host to be prepended to every log message.
   * @param programName The name of the program to be prepended to every log
   * message.
   */
  TestingSyslogLogger(const std::string &hostName, const std::string &programName):
    SyslogLogger(hostName, programName, DEBUG)  {
  }

  using SyslogLogger::cleanString;

}; // class TestingSyslogLogger

} // namespace log
} // namespace cta

