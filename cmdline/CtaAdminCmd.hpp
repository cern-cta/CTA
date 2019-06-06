/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Command-line tool for CTA Admin commands
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
 * @copyright      Copyright 2017 CERN
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

#pragma once

#include "CtaAdminCmdParse.hpp"

namespace cta {
namespace admin {

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

   // Static method to convert time to string
   static std::string timeToString(const time_t &time)
   {
      std::string timeString(ctime(&time));
      timeString.resize(timeString.size()-1); //remove newline
      return timeString;
   }

   // Output headers
   static void printAfLsHeader();
   static void printAfLsSummaryHeader();
   static void printFrLsHeader();
   static void printFrLsSummaryHeader();
   static void printLpaHeader();
   static void printLpaSummaryHeader();
   static void printLprHeader();
   static void printLprSummaryHeader();
   static void printTpLsHeader();
   static void printTapeLsHeader();
   static void printRepackLsHeader();
   
   // Output records
   static void print(const ArchiveFileLsItem &afls_item);
   static void print(const ArchiveFileLsSummary &afls_summary);
   static void print(const FailedRequestLsItem &frls_item);
   static void print(const FailedRequestLsSummary &frls_summary);
   static void print(const ListPendingArchivesItem &lpa_item);
   static void print(const ListPendingArchivesSummary &lpa_summary);
   static void print(const ListPendingRetrievesItem &lpr_item);
   static void print(const ListPendingRetrievesSummary &lpr_summary);
   static void print(const TapePoolLsItem &tpls_item);
   static void print(const TapeLsItem &tals_item);
   static void print(const RepackLsItem &rels_item);

private:
   //! Parse the options for a specific command/subcommand
   void parseOptions(int start, int argc, const char *const *const argv, const cmd_val_t &options);

   //! Add a valid option to the protocol buffer
   void addOption(const Option &option, const std::string &value);

   //! Read a list of string options from a file
   void readListFromFile(cta::admin::OptionStrList &str_list, const std::string &filename);

   //! Throw an exception with usage help
   void throwUsage(const std::string &error_txt = "") const;

   //! Return the request timeout value (to pass to the ServiceClientSide constructor)
   int GetRequestTimeout() const;

   // Member variables

   const std::string StreamBufferSize      = "1024";                  //!< Buffer size for Data/Stream Responses
   const std::string DefaultRequestTimeout = "10";                    //!< Default Request Timeout. Can be overridden by
                                                                      //!< XRD_REQUESTTIMEOUT environment variable.

   std::string       m_execname;                                      //!< Executable name of this program
   cta::xrd::Request m_request;                                       //!< Protocol Buffer for the command and parameters

   static std::atomic<bool> is_json;                                  //!< Display results in JSON format
   static std::atomic<bool> is_first_record;                          //!< Delimiter for JSON records

   static constexpr const char* const TEXT_RED    = "\x1b[31;1m";     //!< Terminal formatting code for red text
   static constexpr const char* const TEXT_NORMAL = "\x1b[0m";        //!< Terminal formatting code for normal text
   static constexpr const char* const LOG_SUFFIX  = "CtaAdminCmd";    //!< Identifier for log messages
};

}} // namespace cta::admin
