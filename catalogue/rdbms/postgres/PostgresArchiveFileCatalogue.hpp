/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>

#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "common/checksum/ChecksumBlob.hpp"

namespace cta::catalogue {

class TapeFileWritten;
class RdbmsCatalogue;

class PostgresArchiveFileCatalogue : public RdbmsArchiveFileCatalogue {
public:
  PostgresArchiveFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresArchiveFileCatalogue() override = default;

  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName, const uint64_t archiveFileId,
    log::LogContext &lc) override;

private:
  uint64_t getNextArchiveFileId(rdbms::Conn &conn) override;

  void copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn & conn,
    const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) override;

  /**
   * The size and checksum of a file.
   */
  struct FileSizeAndChecksum {
    uint64_t fileSize;
    checksum::ChecksumBlob checksumBlob;
  };

  friend class PostgresTapeFileCatalogue;

  /**
   * Returns the sizes and checksums of the specified archive files.
   *
   * @param conn The database connection.
   * @param events The tape file written events that identify the archive files.
   * @return A map from the identifier of each archive file to its size and checksum.
   */
  std::map<uint64_t, FileSizeAndChecksum> selectArchiveFileSizesAndChecksums(rdbms::Conn &conn,
    const std::set<TapeFileWritten> &events) const;
};  // class PostgresArchiveFileCatalogue

} // namespace cta::catalogue
