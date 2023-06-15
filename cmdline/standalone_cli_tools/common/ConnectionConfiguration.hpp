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

#pragma once

#include "eos_grpc_client/GrpcEndpoint.hpp"
#include "CtaFrontendApi.hpp"

#include <string>
#include <tuple>

namespace cta {

namespace log {
class StdoutLogger;
}

namespace cliTool {

class CmdLineArgs;

class ConnConfiguration {
public:
  /**
   * Sets internal configuration parameters to be used for reading.
   * It reads cta frontend parameters from /etc/cta/cta-cli.conf
   *
   * @param username The name of the user running the command-line tool.
   * @param cmdLineArgs The arguments parsed from the command line.
   * @param serviceProviderPtr Xroot service provider
   * @param endpointMapPtr Endpoint for communication with EOS
   */
  using XrdSsiPbServiceTypePtr = std::unique_ptr<XrdSsiPbServiceType>;
  using EndpointMapPtr = std::unique_ptr<::eos::client::EndpointMap>;
  static std::tuple<XrdSsiPbServiceTypePtr, EndpointMapPtr>
    readAndSetConfiguration(cta::log::StdoutLogger& log, const std::string& userName, const CmdLineArgs& cmdLineArgs);

private:
  static EndpointMapPtr setNamespaceMap(const std::string& keytab_file);
};

}  // namespace cliTool
}  // namespace cta