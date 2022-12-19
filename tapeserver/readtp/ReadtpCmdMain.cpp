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

#include <iostream>

#include "common/exception/Exception.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/EncryptionControl.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tapeserver/readtp/ReadtpCmd.hpp"

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {

  const std::string DAEMON_CONFIG = "/etc/cta/cta-taped.conf";

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
  cta::mediachanger::MediaChangerFacade mc(log);

  bool useEncryption;
  std::string externalEncryptionKeyScript;

  try {
    // Config file needed to find the cta-get-encryption-key script
    const cta::tape::daemon::TapedConfiguration tapedConfig =
      cta::tape::daemon::TapedConfiguration::createFromCtaConf(DAEMON_CONFIG, dummyLog);
    externalEncryptionKeyScript = tapedConfig.externalEncryptionKeyScript.value();
    useEncryption = tapedConfig.useEncryption.value() == "yes";

    cta::tapeserver::readtp::ReadtpCmd cmd(std::cin, std::cout, std::cerr, log, dummyLog, mc, useEncryption, externalEncryptionKeyScript);
    return cmd.main(argc, argv);
  } catch(cta::exception::Exception &ex) {
    std::cerr << ex.getMessageValue() << std::endl;
    return 1;
  }
}