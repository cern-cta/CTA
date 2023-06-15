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

#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tapeserver/tapelabel/TapeLabelCmd.hpp"

#include <iostream>

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char* const* const argv) {
  const std::string tapeConfigFile = "/etc/cta/cta-taped.conf";

  char buf[256];
  std::string hostName;
  if (gethostname(buf, sizeof(buf))) {
    hostName = "UNKNOWN";
  }
  else {
    buf[sizeof(buf) - 1] = '\0';
    hostName = buf;
  }
  cta::log::StdoutLogger log(hostName, "cta-tape-label");
  const cta::tape::daemon::TapedConfiguration tapeConfig =
    cta::tape::daemon::TapedConfiguration::createFromCtaConf(tapeConfigFile);
  cta::mediachanger::RmcProxy rmcProxy(tapeConfig.rmcPort.value(), tapeConfig.rmcNetTimeout.value(),
                                       tapeConfig.rmcRequestAttempts.value());
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, log);

  cta::tapeserver::tapelabel::TapeLabelCmd cmd(std::cin, std::cout, std::cerr, log, mc);
  return cmd.main(argc, argv);
}
