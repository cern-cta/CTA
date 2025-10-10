/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <iostream>

#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "tapeserver/readtp/ReadtpCmd.hpp"

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
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

  cta::tapeserver::readtp::ReadtpCmd cmd(std::cin, std::cout, std::cerr, log, dummyLog);
  return cmd.main(argc, argv);
}
