/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  static std::tuple<XrdSsiPbServiceTypePtr, EndpointMapPtr> readAndSetConfiguration(
    cta::log::StdoutLogger &log,
    const std::string &userName,
    const CmdLineArgs &cmdLineArgs);

private:
  static EndpointMapPtr setNamespaceMap(const std::string &keytab_file);
};

} // namespace cliTool
} // namespace cta
