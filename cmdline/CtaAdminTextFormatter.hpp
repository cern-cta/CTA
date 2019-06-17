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
  void printAfLsHeader();
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
  void print(const ArchiveFileLsItem &afls_item);
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
  //! Add a line to the buffer
  template<typename... Args>
  void push_back(Args... args) {
    std::vector<std::string> line;
    buildVector(line, args...);
    m_outputBuffer.push_back(line);
    if(m_outputBuffer.size() >= m_bufLines) flush();
  }

  //! Recursive variadic function to build a log string from an arbitrary number of items of arbitrary type
  template<typename T, typename... Args>
  void buildVector(std::vector<std::string> &line, const T &item, Args... args) {
    buildVector(line, item);
    buildVector(line, args...);
  }

  //! Base case function to add one item to the log
  template<typename T>
  void buildVector(std::vector<std::string> &line, const T &item) {
    line.push_back(std::to_string(item));
  }

  void buildVector(std::vector<std::string> &line, const std::string &item) {
    line.push_back(item);
  }

  void buildVector(std::vector<std::string> &line, const char *item) {
    line.push_back(std::string(item));
  }

  void flush();                                            //!< Flush buffer to stdout

  unsigned int m_bufLines;                                 //!< Number of text lines to buffer before flushing formatted output
  std::vector<std::vector<std::string>> m_outputBuffer;    //!< Buffer for text output (not used for JSON)

  static const std::string TEXT_RED;                       //!< Terminal formatting code for red text
  static const std::string TEXT_NORMAL;                    //!< Terminal formatting code for normal text
};

}}
