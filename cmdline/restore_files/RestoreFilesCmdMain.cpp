/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <sstream>
#include <iostream>

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include "RestoreFilesCmd.hpp"
#include "common/utils/utils.hpp"
#include "common/log/StdoutLogger.hpp"

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
  char buf[256];
  std::string hostName;
  if(gethostname(buf, sizeof(buf))) {
    hostName = "UNKNOWN";
  } else {
    buf[sizeof(buf) - 1] = '\0';
    hostName = buf;
  }
  cta::log::StdoutLogger log(hostName, "cta-restore-deleted-files");

  cta::admin::RestoreFilesCmd cmd(std::cin, std::cout, std::cerr, log);
  int ret = cmd.main(argc, argv);
  // Delete all global objects allocated by libprotobuf
  google::protobuf::ShutdownProtobufLibrary();
  return ret;
}