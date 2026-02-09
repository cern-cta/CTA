/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "tools/standalone_cli_tools/change_storage_class/ChangeStorageClass.hpp"

#include <iostream>
#include <sstream>

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char* const* const argv) {
  std::string hostName = std::getenv("HOSTNAME");
  if (hostName.empty()) {
    hostName = "UNKNOWN";
  }

  cta::log::StdoutLogger log(hostName, "cta-change-storage-class");

  cta::cliTool::ChangeStorageClass cmd(std::cin, std::cout, std::cerr, log);
  int ret = cmd.mainImpl(argc, argv);
  // Delete all global objects allocated by libprotobuf
  google::protobuf::ShutdownProtobufLibrary();
  return ret;
}
