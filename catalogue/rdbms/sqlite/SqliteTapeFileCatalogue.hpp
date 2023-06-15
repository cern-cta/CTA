/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <string>

#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"

namespace cta {

namespace utils {
class Timer;
}

namespace log {
class TimingList;
}

namespace catalogue {

class RdbmsCatalogue;

class SqliteTapeFileCatalogue : public RdbmsTapeFileCatalogue {
public:
  SqliteTapeFileCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue);
  ~SqliteTapeFileCatalogue() override = default;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) override;

private:
  void copyTapeFileToFileRecyleLogAndDeleteTransaction(rdbms::Conn& conn,
                                                       const cta::common::dataStructures::ArchiveFile& file,
                                                       const std::string& reason,
                                                       utils::Timer* timer,
                                                       log::TimingList* timingList,
                                                       log::LogContext& lc) const override;

  void fileWrittenToTape(rdbms::Conn& conn, const TapeFileWritten& event);
};  // class SqliteTapeFileCatalogue

}  // namespace catalogue
}  // namespace cta
