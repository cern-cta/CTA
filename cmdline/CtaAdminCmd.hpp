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

#include "CtaAdminCmdParse.hpp"
#include "version.h"

namespace cta::admin {

class CtaAdminCmd
{
public:
   CtaAdminCmd(int argc, const char *const *const argv);

   //! Send the protocol buffer across the XRootD SSI transport
   void send() const;

   // Static methods to format streaming responses
   static bool isJson() { return is_json; }
   static char jsonDelim() {
      char c = is_first_record ? '[' : ',';
      is_first_record = false;
      return c;
   }
   static std::string jsonCloseDelim() {
      return is_first_record ? "[]" : "]";
   }

private:
   //! Parse the options for a specific command/subcommand
   void parseOptions(int start, int argc, const char *const *const argv, const cmd_val_t &options);

   //! Add a valid option to the protocol buffer
   void addOption(const Option &option, const std::string &value);

   //! Parses the TPCONFIG file to obtain the first drive in the tapeserver
   std::string getDriveFromTpConfig();

   //! Read a list of string options from a file
   void readListFromFile(cta::admin::OptionStrList &str_list, const std::string &filename);

   //! Throw an exception with usage help
   void throwUsage(const std::string &error_txt = "") const;

   //! Return the request timeout value (to pass to the ServiceClientSide constructor)
   int GetRequestTimeout() const;

   //! Returns user config path if specified, if not looks in $HOME/.cta, then in /etc/cta
   const std::string getConfigFilePath() const;

   // Member variables

   const std::string StreamBufferSize      = "1024";                  //!< Buffer size for Data/Stream Responses
   const std::string DefaultRequestTimeout = "10";                    //!< Default Request Timeout. Can be overridden by
                                                                      //!< XRD_REQUESTTIMEOUT environment variable.

   std::string       m_execname;                                      //!< Executable name of this program
   cta::xrd::Request m_request;                                       //!< Protocol Buffer for the command and parameters
   
   const std::string m_ctaVersion = CTA_VERSION;                      //!< The version of CTA
   const std::string m_xrootdSsiProtobufInterfaceVersion =            //!< The xrootd-ssi-protobuf-interface version (=tag)
      XROOTD_SSI_PROTOBUF_INTERFACE_VERSION;
   
   static std::atomic<bool> is_json;                                  //!< Display results in JSON format
   static std::atomic<bool> is_first_record;                          //!< Delimiter for JSON records

   std::optional<std::string> m_config;                               //!< User defined config file

   static constexpr const char* const LOG_SUFFIX  = "CtaAdminCmd";    //!< Identifier for log messages
};

} // namespace cta::admin
