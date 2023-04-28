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

#include <optional>
#include <set>
#include <string>

#include "common/exception/UserError.hpp"

namespace cta {

namespace common {
namespace dataStructures {
struct ArchiveFile;
struct RequesterIdentity;
struct RetrieveFileQueueCriteria;
}
}

namespace log {
struct LogContext;
}

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedExistingDeletedFileCopy);

class TapeItemWrittenPointer;

/**
 * Specifies the interface to a factory Catalogue objects.
 */
class TapeFileCatalogue {
public:
  virtual ~TapeFileCatalogue() = default;

  /**
   * Notifies the catalogue that the specified files have been written to tape.
   *
   * @param events The tape file written events.
   * @throw TapeFseqMismatch If an unexpected tape file sequence number is encountered.
   * @throw FileSizeMismatch If an unexpected tape file size is encountered.
   */
  virtual void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) = 0;

  /**
  * Deletes a tape file copy
  *
  * @param file The tape file to delete
  * @param reason The reason for deleting the tape file copy
  */
  virtual void deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) = 0;

  /**
   * Prepares for a file retrieval by returning the information required to
   * queue the associated retrieve request(s).
   *
   * @param diskInstanceName The name of the instance from where the retrieval
   * request originated
   * @param archiveFileId The unique identifier of the archived file that is
   * to be retrieved.
   * @param user The user for whom the file is to be retrieved.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * retrieving the file.
   * @param activity The activity under which the user wants to start the retrieve
   * The call will fail if the activity is set and unknown.
   * @param lc The log context.
   *
   * @return The information required to queue the associated retrieve request(s).
   */
  virtual common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(
    const std::string &diskInstanceName,
    const uint64_t archiveFileId,
    const common::dataStructures::RequesterIdentity &user,
    const std::optional<std::string> & activity,
    log::LogContext &lc,
    const std::optional<std::string> &mountPolicyName =  std::nullopt) = 0;
};  // class FileRecyleLogCatalogue

}  // namespace catalogue
}  // namespace cta
