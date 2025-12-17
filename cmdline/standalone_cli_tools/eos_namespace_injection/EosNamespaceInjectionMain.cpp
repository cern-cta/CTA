/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "cmdline/standalone_cli_tools/eos_namespace_injection/EosNamespaceInjection.hpp"
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
  std::optional<std::string> hostName = std::getenv("HOSTNAME");
  if (!hostName) {
    hostName = "UNKNOWN";
  }

  cta::log::StdoutLogger log(hostName.value(), "cta-eos-namespace-injection");

  cta::cliTool::EosNamespaceInjection cmd(std::cin, std::cout, std::cerr, log);
  int ret = cmd.mainImpl(argc, argv);
  // Delete all global objects allocated by libprotobuf
  google::protobuf::ShutdownProtobufLibrary();
  return ret;
}
