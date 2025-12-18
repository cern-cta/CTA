/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ConnectionConfiguration.hpp"

#include "cmdline/standalone_cli_tools/common/CmdLineArgs.hpp"
#include "common/log/StdoutLogger.hpp"

#include <string>
#include <utility>

namespace cta::cliTool {

std::unique_ptr<::eos::client::EndpointMap> ConnConfiguration::setNamespaceMap(const std::string& keytab_file) {
  // Open the keytab file for reading
  std::ifstream file(keytab_file);
  if (!file) {
    throw cta::exception::UserError("Failed to open namespace keytab configuration file " + keytab_file);
  }
  ::eos::client::NamespaceMap_t namespaceMap;
  // Parse the keytab line by line
  std::string line;
  for (int lineno = 1; std::getline(file, line); ++lineno) {
    // Strip out comments
    if (auto pos = line.find('#'); pos != std::string::npos) {
      line.resize(pos);
    }

    // Parse one line
    std::stringstream ss(line);
    std::string diskInstance;
    std::string endpoint;
    std::string token;
    std::string eol;
    ss >> diskInstance >> endpoint >> token >> eol;

    // Ignore blank lines, all other lines must have exactly 3 elements
    if (token.empty() || !eol.empty()) {
      if (diskInstance.empty() && endpoint.empty() && token.empty()) {
        continue;
      }
      throw cta::exception::UserError("Could not parse namespace keytab configuration file line "
                                      + std::to_string(lineno) + ": " + line);
    }
    namespaceMap.insert(std::make_pair(diskInstance, ::eos::client::Namespace(endpoint, token)));
  }
  return std::make_unique<::eos::client::EndpointMap>(namespaceMap);
}

//------------------------------------------------------------------------------
// readAndSetConfiguration
//------------------------------------------------------------------------------
std::tuple<std::unique_ptr<XrdSsiPbServiceType>, std::unique_ptr<::eos::client::EndpointMap>>
ConnConfiguration::readAndSetConfiguration(cta::log::StdoutLogger& log,
                                           const std::string& userName,
                                           const CmdLineArgs& cmdLineArgs) {
  const std::string StreamBufferSize = "1024";     //!< Buffer size for Data/Stream Responses
  const std::string DefaultRequestTimeout = "10";  //!< Default Request Timeout. Can be overridden by
                                                   //!< XRD_REQUESTTIMEOUT environment variable.
  if (cmdLineArgs.m_debug) {
    log.setLogMask("DEBUG");
  } else {
    log.setLogMask("INFO");
  }

  // Set CTA frontend configuration options
  const std::string cli_config_file = "/etc/cta/cta-cli.conf";
  XrdSsiPb::Config cliConfig(cli_config_file, "cta");
  cliConfig.set("resource", "/ctafrontend");
  cliConfig.set("response_bufsize", StreamBufferSize);      // default value = 1024 bytes
  cliConfig.set("request_timeout", DefaultRequestTimeout);  // default value = 10s

  // Allow environment variables to override config file
  cliConfig.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

  // If XRDDEBUG=1, switch on all logging
  if (getenv("XRDDEBUG")) {
    cliConfig.set("log", "all");
  }
  // If fine-grained control over log level is required, use XrdSsiPbLogLevel
  cliConfig.getEnv("log", "XrdSsiPbLogLevel");

  // Validate that endpoint was specified in the config file
  if (!cliConfig.getOptionValueStr("endpoint").first) {
    throw std::runtime_error("Configuration error: cta.endpoint missing from " + cli_config_file);
  }

  // If the server is down, we want an immediate failure. Set client retry to a single attempt.
  XrdSsiProviderClient->SetTimeout(XrdSsiProvider::connect_N, 1);

  std::unique_ptr<XrdSsiPbServiceType> serviceProviderPtr = std::make_unique<XrdSsiPbServiceType>(cliConfig);

  // Set CTA frontend configuration options to connect to eos
  const std::string frontend_xrootd_config_file = "/etc/cta/cta-frontend-xrootd.conf";
  XrdSsiPb::Config frontendXrootdConfig(frontend_xrootd_config_file, "cta");

  // Get the endpoint for namespace queries
  auto nsConf = frontendXrootdConfig.getOptionValueStr("ns.config");
  if (nsConf.first) {
    auto endpointMap = setNamespaceMap(nsConf.second);
    std::pair<std::unique_ptr<XrdSsiPbServiceType>, std::unique_ptr<::eos::client::EndpointMap>>
      serviceProviderPtrAndEndpointMap = std::make_pair(std::move(serviceProviderPtr), std::move(endpointMap));
    return serviceProviderPtrAndEndpointMap;
  } else {
    throw std::runtime_error("Configuration error: cta.ns.config missing from " + frontend_xrootd_config_file);
  }
}

}  // namespace cta::cliTool
