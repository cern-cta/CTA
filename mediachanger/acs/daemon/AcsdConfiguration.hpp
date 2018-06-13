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
#include <string>
#include <map>
#include <type_traits>
#include <limits>
#include "common/log/DummyLogger.hpp"
#include "common/exception/Exception.hpp"
#include "common/SourcedParameter.hpp"
#include "Tpconfig.hpp"
#include "mediachanger/acs/Constants.hpp"
#include "common/ConfigurationFile.hpp"

#include <time.h>

namespace cta {
namespace mediachanger { 
namespace acs	{
namespace daemon {
/**
 * Class containing all the parameters needed by the watchdog process
 * to spawn a transfer session per drive.
 */
struct AcsdConfiguration {
  static AcsdConfiguration createFromCtaConf(
          const std::string & generalConfigPath,
          cta::log::Logger & log = gDummyLogger);
 
  SourcedParameter<uint64_t> port{
    "acsd", "Port", (uint64_t)ACS_PORT, "Compile time default"};
  SourcedParameter<uint64_t> QueryInterval{ 
    "acsd", "QueryInterval", (long unsigned int)ACS_QUERY_INTERVAL, "Compile time default"};
  SourcedParameter<uint64_t> CmdTimeout{
    "acsd", "CmdTimeout",(long unsigned int) ACS_CMD_TIMEOUT, "Compile time default"};
  SourcedParameter<std::string> daemonUserName{
    "acsd", "DaemonUserName", "cta", "Compile time default"};
  SourcedParameter<std::string> daemonGroupName{
    "acsd", "DaemonGroupName", "tape", "Compile time default"};
private:
  /** A private dummy logger which will simplify the implementation of the 
   * functions (just unconditionally log things). */
  static cta::log::DummyLogger gDummyLogger;
} ;
} // namespace daemon
} // namespace acs
} // namespace mediachanger
} // namespace cta
