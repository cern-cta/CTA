/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  SqliteTapeFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue *rdbmsCatalogue);
  ~SqliteTapeFileCatalogue() override = default;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) override;

private:
  void  copyTapeFileToFileRecyleLogAndDeleteTransaction(rdbms::Conn & conn,
    const cta::common::dataStructures::ArchiveFile &file, const std::string &reason, utils::Timer *timer,
    log::TimingList *timingList, log::LogContext & lc) const override;

  void fileWrittenToTape(rdbms::Conn &conn, const TapeFileWritten &event);
};  // class SqliteTapeFileCatalogue

}} // namespace cta::catalogue
