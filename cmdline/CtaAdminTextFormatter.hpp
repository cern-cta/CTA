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

class CtaAdminTextFormatter
{
public:
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
   // Static method to convert time to string
   static std::string timeToString(const time_t &time)
   {
      std::string timeString(ctime(&time));
      timeString.resize(timeString.size()-1); //remove newline
      return timeString;
   }

   static constexpr const char* const TEXT_RED    = "\x1b[31;1m";     //!< Terminal formatting code for red text
   static constexpr const char* const TEXT_NORMAL = "\x1b[0m";        //!< Terminal formatting code for normal text
};

}}
