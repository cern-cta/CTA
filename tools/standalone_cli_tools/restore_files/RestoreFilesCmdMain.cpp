/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RestoreFilesCmd.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"

#include <XrdSsiPbIStreamBuffer.hpp>
#include <XrdSsiPbLog.hpp>
#include <iostream>
#include <sstream>

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
  cta::log::StdoutLogger log(hostName, "cta-restore-deleted-files");

  cta::cliTool::RestoreFilesCmd cmd(std::cin, std::cout, std::cerr, log);
  int ret = cmd.mainImpl(argc, argv);
  // Delete all global objects allocated by libprotobuf
  google::protobuf::ShutdownProtobufLibrary();
  return ret;
}
