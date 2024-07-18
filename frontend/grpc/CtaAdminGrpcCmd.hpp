/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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

#include "cmdline/CtaAdminCmdParse.hpp"
#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "common/log/Logger.hpp"

#include <string>
#include <atomic>

namespace cta::frontend::grpc::client {

class CtaAdminGrpcCmd {
  
public:
  CtaAdminGrpcCmd(int argc, char** argv);
  ~CtaAdminGrpcCmd() = default;
  void exe(cta::log::Logger& log, const std::string& strSslRoot,
           const std::string& strSslKey, const std::string& strSslCert,
           const std::string& strGrpcHost, const unsigned int& uiGrpcPort);
  
  // Static methods to format streaming responses
  static bool isJson() { return m_abIsJson; }

private:
  std::string m_strExecname;
  cta::xrd::Request m_request;                 //!< Protocol Buffer for the command and parameters
  static std::atomic<bool> m_abIsJson;         //!< Display results in JSON format

  
  //! Parse the options for a specific command/subcommand
  void parseOptions(int start, int argc, char** argv, const cta::admin::cmd_val_t& options);

  //! Add a valid option to the protocol buffer
  void addOption(const cta::admin::Option& option, const std::string& strValue);
  
  //! Read a list of string options from a file
  void readListFromFile(cta::admin::OptionStrList &str_list, const std::string &filename);
  
  //! Throw an exception with usage help
  void throwUsage(const std::string &strError = "") const;
  
};

} // namespace cta::frontend::grpc::client
