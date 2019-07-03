/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Text formatter for CTA Admin command tool
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
 * @copyright      Copyright 2019 CERN
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

#include "CtaFrontendApi.hpp"

namespace cta {
namespace admin {

class TextFormatter
{
public:
  /*!
   * Constructor
   *
   * @param[in]  bufLines  Number of text lines to buffer before flushing formatted output
   *                       (Not used for JSON output which does not need to be formatted
   *                        so can be streamed directly)
   */
  TextFormatter(unsigned int bufLines = 1000) :
    m_bufLines(bufLines) {
    m_outputBuffer.reserve(bufLines);
  }

  ~TextFormatter() {
    flush();
  }

  // Output headers
  void printAdminLsHeader();
  void printArchiveFileLsHeader();
  void printArchiveFileLsSummaryHeader();
  void printArchiveRouteLsHeader();
  void printDriveLsHeader();
  void printFailedRequestLsHeader();
  void printFailedRequestLsSummaryHeader();
  void printGroupMountRuleLsHeader();
  void printListPendingArchivesHeader();
  void printListPendingArchivesSummaryHeader();
  void printListPendingRetrievesHeader();
  void printListPendingRetrievesSummaryHeader();
  void printLogicalLibraryLsHeader();
  void printMountPolicyLsHeader();
  void printRepackLsHeader();
  void printRequesterMountRuleLsHeader();
  void printShowQueuesHeader();
  void printStorageClassLsHeader();
  void printTapeLsHeader();
  void printTapePoolLsHeader();
  void printDiskSystemLsHeader();
   
  // Output records
  void print(const AdminLsItem &adls_item);
  void print(const ArchiveFileLsItem &afls_item);
  void print(const ArchiveFileLsSummary &afls_summary);
  void print(const ArchiveRouteLsItem &afls_item);
  void print(const DriveLsItem &drls_item);
  void print(const FailedRequestLsItem &frls_item);
  void print(const FailedRequestLsSummary &frls_summary);
  void print(const GroupMountRuleLsItem &gmrls_item);
  void print(const ListPendingArchivesItem &lpa_item);
  void print(const ListPendingArchivesSummary &lpa_summary);
  void print(const ListPendingRetrievesItem &lpr_item);
  void print(const ListPendingRetrievesSummary &lpr_summary);
  void print(const LogicalLibraryLsItem &llls_item);
  void print(const MountPolicyLsItem &mpls_item);
  void print(const RepackLsItem &rels_item);
  void print(const RequesterMountRuleLsItem &rmrls_item);
  void print(const ShowQueuesItem &sq_item);
  void print(const StorageClassLsItem &scls_item);
  void print(const TapeLsItem &tals_item);
  void print(const TapePoolLsItem &tpls_item);
  void print(const DiskSystemLsItem &dsls_item);
  

private:
  //! Add a line to the buffer
  template<typename... Args>
  void push_back(Args... args) {
    std::vector<std::string> line;
    buildVector(line, args...);
    m_outputBuffer.push_back(line);
    if(m_outputBuffer.size() >= m_bufLines) flush();
  }

  //! Recursive variadic method to build a log string from an arbitrary number of items of arbitrary type
  template<typename T, typename... Args>
  static void buildVector(std::vector<std::string> &line, const T &item, Args... args) {
    buildVector(line, item);
    buildVector(line, args...);
  }

  //! Base case method to add one item to the log
  static void buildVector(std::vector<std::string> &line, const std::string &item) {
    line.push_back(item);
  }

  //! Base case method to add one item to the log, overloaded for char*
  static void buildVector(std::vector<std::string> &line, const char *item) {
    line.push_back(std::string(item));
  }

  //! Base case method to add one item to the log, overloaded for bool
  static void buildVector(std::vector<std::string> &line, bool item) {
    line.push_back(item ? "true" : "false");
  }

  /*!
   * Base case method to add one item to the log, with partial specialisation
   * (works for all integer and floating-point types)
   */
  template<typename T>
  static void buildVector(std::vector<std::string> &line, const T &item) {
    line.push_back(std::to_string(item));
  }

  //! Convert double to string with one decimal place precision and a suffix
  static std::string doubleToStr(double value, char unit);

  //! Convert UNIX time to string
  static std::string timeToStr(const time_t &unixtime);

  //! Convert data size in bytes to abbreviated string with appropriate size suffix (K/M/G/T/P/E)
  static std::string dataSizeToStr(uint64_t value);

  //! Flush buffer to stdout
  void flush();

  unsigned int m_bufLines;                                          //!< Number of text lines to buffer before flushing formatted output
  std::vector<std::vector<std::string>> m_outputBuffer;             //!< Buffer for text output (not used for JSON)

  static constexpr const char* const TEXT_RED    = "\x1b[31;1m";    //!< Terminal formatting code for red text
  static constexpr const char* const TEXT_NORMAL = "\x1b[0m";       //!< Terminal formatting code for normal text
};

}}
