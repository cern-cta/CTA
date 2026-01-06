/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/CmdLineParams.hpp"
#include "common/process/threading/Daemon.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"

namespace cta::tape::daemon {

/** Daemon responsible for reading and writing data from and to one or more tape
 * drives drives connected to a tape server. */

class TapeDaemon {
public:
  /** Constructor.
   * @param commandLine The parameters extracted from the command line.
   * @param log The object representing the API of the CTA logging system.
   * @param globalConfig The configuration of the tape server. */
  TapeDaemon(const cta::common::CmdLineParams& commandLine,
             cta::log::Logger& log,
             const common::TapedConfiguration& globalConfig);

  /** Runs the daemon. */
  void run();

protected:
  /** Sets the dumpable attribute of the current process to true. */
  void setDumpable();

  /** The tape server's configuration */
  const common::TapedConfiguration& m_globalConfiguration;

  /**
   * The program name of the daemon.
   */
  const std::string m_programName {"cta-taped"};
};  // class TapeDaemon

}  // namespace cta::tape::daemon
