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

#include <map>
#include <memory>
#include <set>
#include <string>

#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "common/checksum/ChecksumBlob.hpp"

namespace cta {

namespace catalogue {

class TapeFileWritten;
class RdbmsCatalogue;

class PostgresArchiveFileCatalogue : public RdbmsArchiveFileCatalogue {
public:
  PostgresArchiveFileCatalogue(log::Logger& log,
                               std::shared_ptr<rdbms::ConnPool> connPool,
                               RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresArchiveFileCatalogue() override = default;

  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string& diskInstanceName,
                                               const uint64_t archiveFileId,
                                               log::LogContext& lc) override;

private:
  uint64_t getNextArchiveFileId(rdbms::Conn& conn) override;

  void copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn& conn,
                                               const common::dataStructures::DeleteArchiveRequest& request,
                                               log::LogContext& lc) override;

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
  std::map<uint64_t, FileSizeAndChecksum>
    selectArchiveFileSizesAndChecksums(rdbms::Conn& conn, const std::set<TapeFileWritten>& events) const;
};  // class PostgresArchiveFileCatalogue

}  // namespace catalogue
}  // namespace cta
