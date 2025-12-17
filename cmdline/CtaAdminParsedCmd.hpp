/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminCmdParser.hpp"
#include "version.h"

namespace cta::admin {

class CtaAdminParsedCmd {
public:
  CtaAdminParsedCmd(int argc, const char* const* const argv);

  // Static methods to format streaming responses
  static bool isJson() { return is_json || is_json_lines; }

  static std::string jsonDelim() {
    std::string c = is_json_lines ? (is_first_record ? "" : "\n") : (is_first_record ? "[" : ",");
    is_first_record = false;
    return c;
  }

  static std::string jsonCloseDelim() {
    return is_json_lines ? (is_first_record ? "" : "\n") : (is_first_record ? "[]\n" : "]\n");
  }

  //! Throw an exception with usage help
  void throwUsage(const std::string& error_txt = "") const;

  //! Returns user config path if specified, if not looks in $HOME/.cta, then in /etc/cta
  const std::string getConfigFilePath() const;

  const cta::xrd::Request& getRequest() const { return m_request; }

private:
  //! Parse the options for a specific command/subcommand
  void parseOptions(int start, int argc, const char* const* const argv, const cmd_val_t& options);

  //! Add a valid option to the protocol buffer
  void addOption(const Option& option, const std::string& value);

  //! Read a list of string options from a file
  void readListFromFile(cta::admin::OptionStrList& str_list, const std::string& filename);

  // Member variables

  std::string m_execname;       //!< Executable name of this program
  cta::xrd::Request m_request;  //!< Protocol Buffer for the command and parameters

  const std::string m_ctaVersion = CTA_VERSION;            //!< The version of CTA
  const std::string m_xrootdSsiProtobufInterfaceVersion =  //!< The xrootd-ssi-protobuf-interface version (=tag)
    XROOTD_SSI_PROTOBUF_INTERFACE_VERSION;

  static std::atomic<bool> is_json;          //!< Display results in JSON format
  static std::atomic<bool> is_json_lines;    //!< Display results in JSON Lines format
  static std::atomic<bool> is_first_record;  //!< Delimiter for JSON records

  std::optional<std::string> m_config;  //!< User defined config file

  static constexpr const char* const LOG_SUFFIX = "CtaAdminParsedCmd";  //!< Identifier for log messages
};

}  // namespace cta::admin
