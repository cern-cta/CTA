/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"

namespace cta::catalogue {

class RdbmsCatalogue;

class SqliteArchiveFileCatalogue : public RdbmsArchiveFileCatalogue {
public:
  SqliteArchiveFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~SqliteArchiveFileCatalogue() override = default;

  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName, const uint64_t archiveFileId,
    log::LogContext &lc) override;

private:
  uint64_t getNextArchiveFileId(rdbms::Conn &conn) override;

  void copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn & conn,
    const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) override;
};  // class SqliteArchiveFileCatalogue

} // namespace cta::catalogue
