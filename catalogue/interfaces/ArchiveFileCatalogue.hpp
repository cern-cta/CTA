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

#include <stdint.h>

#include <list>
#include <string>

#include "catalogue/TapeFileSearchCriteria.hpp"

namespace cta {

namespace common::dataStructures {
struct ArchiveFile;
struct ArchiveFileQueueCriteria;
struct ArchiveFileSummary;
struct DeleteArchiveRequest;
struct RequesterIdentity;
}

namespace log {
struct LogContext;
}

namespace catalogue {

template <typename Item>
class CatalogueItor;

using ArchiveFileItor = CatalogueItor<common::dataStructures::ArchiveFile>;

class ArchiveFileCatalogue {
public:
  virtual ~ArchiveFileCatalogue() = default;

  /**
   * Checks the specified archival could take place and returns a new and
   * unique archive file identifier that can be used by a new archive file
   * within the catalogue.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * storage class belongs.
   * @param storageClassName The name of the storage class of the file to be
   * archived.  The storage class name is only guaranteed to be unique within
   * its disk instance.  The storage class name will be used by the Catalogue
   * to determine the destination tape pool for each tape copy.
   * @param user The user for whom the file is to be archived.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * archiving the file.
   * @return The new archive file identifier.
   * @throw UserErrorWithCacheInfo if there was a user error.
   */
  virtual uint64_t checkAndGetNextArchiveFileId(
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const common::dataStructures::RequesterIdentity &user) = 0;

  /**
   * Returns the information required to queue an archive request.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * storage class belongs.
   * @param storageClassName The name of the storage class of the file to be
   * archived.  The storage class name is only guaranteed to be unique within
   * its disk instance.  The storage class name will be used by the Catalogue
   * to determine the destination tape pool for each tape copy.
   * @param user The user for whom the file is to be archived.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * archiving the file.
   * @return The information required to queue an archive request.
   */
  virtual common::dataStructures::ArchiveFileQueueCriteria getArchiveFileQueueCriteria(
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const common::dataStructures::RequesterIdentity &user) = 0;

  /**
   * Returns the specified archive files.  Please note that the list of files
   * is ordered by archive file ID.
   *
   * @param searchCriteria The search criteria.
   * @return The archive files.
   */
  virtual ArchiveFileItor getArchiveFilesItor(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const = 0;

  /**
   * Returns the specified archive file. If the search criteria result in more than one tape file being returned
   * an exception is thrown.
   * @param searchCriteria The search criteria.
   * @return The archive file.
   */
  virtual common::dataStructures::ArchiveFile getArchiveFileForDeletion(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const = 0;

  /**
   * Returns the specified files in tape file sequence order.
   *
   * @param vid The volume identifier of the tape.
   * @param startFSeq The file sequence number of the first file.  Please note
   * that there might not be a file with this exact file sequence number.
   * @param maxNbFiles The maximum number of files to be returned.
   * @return The specified files in tape file sequence order.
   */
  virtual std::list<common::dataStructures::ArchiveFile> getFilesForRepack(
    const std::string &vid,
    const uint64_t startFSeq,
    const uint64_t maxNbFiles) const = 0;

  /**
   * Returns all the tape copies (no matter their VIDs) of the archive files
   * associated with the tape files on the specified tape in FSEQ order
   * starting at the specified startFSeq.
   *
   * @param vid The volume identifier of the tape.
   * @param startFSeq The file sequence number of the first file.  Please note
   * that there might not be a file with this exact file sequence number.
   * @return The specified files in FSEQ order.
   */
  virtual ArchiveFileItor getArchiveFilesForRepackItor(
    const std::string &vid,
    const uint64_t startFSeq) const = 0;

  /**
   * Returns a summary of the tape files that meet the specified search
   * criteria.
   *
   * @param searchCriteria The search criteria.
   * @return The summary.
   */
  virtual common::dataStructures::ArchiveFileSummary getTapeFileSummary(
    const TapeFileSearchCriteria &searchCriteria = TapeFileSearchCriteria()) const = 0;

  /**
   * Returns the archive file with the specified unique identifier.
   *
   * This method assumes that the archive file being requested exists and will
   * therefore throw an exception if it does not.
   *
   * Please note that an archive file with no associated tape files is
   * considered not to exist by this method.
   *
   * @param id The unique identifier of the archive file.
   * @return The archive file.
   */
  virtual common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) const = 0;

  /**
  * Changes the name of hte storage class
  * @param archiveFileId Id for file found in ARCHIVE_FILE
  * @param newStorageClassName The name of the storage class
  */
  virtual void modifyArchiveFileStorageClassId(const uint64_t archiveFileId,
    const std::string& newStorageClassName) const = 0;

   /**
  * Changes the fxid in for a archive file
  * @param archiveId The archive file id
  * @param fxId The eos fxid related to the archive file
  * @param diskInstance Disk instace
  */
  virtual void modifyArchiveFileFxIdAndDiskInstance(const uint64_t archiveId, const std::string& fxId,
    const std::string &diskInstance) const = 0;

  /**
   * Insert the ArchiveFile and all its tape files in the FILE_RECYCLE_LOG table.
   * There will be one entry on the FILE_RECYCLE_LOG table per deleted tape file
   *
   * @param request the DeleteRequest object that holds information about the file to delete.
   * @param lc the logContext
   */
  virtual void moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
    log::LogContext & lc) = 0;

  /**
   * Updates the disk file ID of the specified archive file.
   *
   * @param archiveFileId The unique identifier of the archive file.
   * @param diskInstance The instance name of the source disk system.
   * @param diskFileId The identifier of the source disk file which is unique
   * within it's host disk system.  Two files from different disk systems may
   * have the same identifier.  The combination of diskInstance and diskFileId
   * must be globally unique, in other words unique within the CTA catalogue.
   */
  virtual void updateDiskFileId(uint64_t archiveFileId, const std::string &diskInstance,
    const std::string &diskFileId) = 0;

  /**
   * !!!!!!!!!!!!!!!!!!! THIS METHOD SHOULD NOT BE USED !!!!!!!!!!!!!!!!!!!!!!!
   * Deletes the specified archive file and its associated tape copies from the
   * catalogue.
   *
   * Please note that the name of the disk instance is specified in order to
   * prevent a disk instance deleting an archive file that belongs to another
   * disk instance.
   *
   * Please note that this method is idempotent.  If the file to be deleted does
   * not exist in the CTA catalogue then this method returns without error.
   *
   * @param instanceName The name of the instance from where the deletion request
   * originated
   * @param archiveFileId The unique identifier of the archive file.
   * @param lc The log context.
   * @return The metadata of the deleted archive file including the metadata of
   * the associated and also deleted tape copies.
   */
  virtual void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &instanceName, const uint64_t archiveFileId,
    log::LogContext &lc) = 0;
};

}} // namespace cta::catalogue
