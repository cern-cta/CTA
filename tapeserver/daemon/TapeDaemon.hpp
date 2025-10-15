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

#include "common/process/threading/Daemon.hpp"
#include "common/CmdLineParams.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"

namespace cta::tape::daemon {

/** Daemon responsible for reading and writing data from and to one or more tape
 * drives drives connected to a tape server. */

class TapeDaemon : public cta::server::Daemon {

public:

  /** Constructor.
   * @param commandLine The parameters extracted from the command line.
   * @param log The object representing the API of the CTA logging system.
   * @param globalConfig The configuration of the tape server. */
  TapeDaemon(
    const cta::common::CmdLineParams & commandLine,
    cta::log::Logger &log,
    const common::TapedConfiguration &globalConfig);

  ~TapeDaemon() final;

  /** The main entry function of the daemon.
   * @return The return code of the process. */
  int mainImpl();

protected:
  /** Exception throwing main() function. */
  void exceptionThrowingMain();

  /** Sets the dumpable attribute of the current process to true. */
  void setDumpable();

  /**
   * The main event loop of the daemon.
   */
  void mainEventLoop();

  /** The tape server's configuration */
  const common::TapedConfiguration& m_globalConfiguration;

  /**
   * The program name of the daemon.
   */
  const std::string m_programName{"cta-taped"};
}; // class TapeDaemon

} // namespace cta::tape::daemon
