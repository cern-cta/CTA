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


#include <sstream>
#include <iostream>

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include "cmdline/standalone_cli_tools/eos_namespace_injection/EosNamespaceInjection.hpp"
#include "common/utils/utils.hpp"
#include "common/log/StdoutLogger.hpp"

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
  std::optional<std::string> hostName = std::getenv("HOSTNAME");
  if(!hostName) {
    hostName = "UNKNOWN";
  }

  cta::log::StdoutLogger log(hostName.value(), "cta-eos-namespace-injection");

  cta::cliTool::EosNamespaceInjection cmd(std::cin, std::cout, std::cerr, log);
  int ret = cmd.main(argc, argv);
  // Delete all global objects allocated by libprotobuf
  google::protobuf::ShutdownProtobufLibrary();
  return ret;
}