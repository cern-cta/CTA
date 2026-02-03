/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "tapeserver/readtp/ReadtpCmd.hpp"

#include <iostream>

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char* const* const argv) {
  char buf[256];
  std::string hostName;
  if (gethostname(buf, sizeof(buf))) {
    hostName = "UNKNOWN";
  } else {
    buf[sizeof(buf) - 1] = '\0';
    hostName = buf;
  }
  cta::log::StdoutLogger log(hostName, "cta-readtp");
  cta::log::DummyLogger dummyLog("dummy", "dummy");

  log.setLogFormat("json");
  dummyLog.setLogFormat("json");

  cta::tapeserver::readtp::ReadtpCmd cmd(std::cin, std::cout, std::cerr, log, dummyLog);
  return cmd.mainImpl(argc, argv);
}
