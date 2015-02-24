#pragma once

#include "ArchivalJob.hpp"
#include "DirectoryIterator.hpp"
#include "LogicalLibrary.hpp"
#include "MigrationRoute.hpp"
#include "RetrievalJob.hpp"
#include "SecurityIdentity.hpp"
#include "StorageClass.hpp"
#include "Tape.hpp"
#include "TapePool.hpp"
#include "UserIdentity.hpp"

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * The user API of the the middle-tier.
 */
class MiddleTierUser {
public:

  /**
   * Destructor.
   */
  virtual ~MiddleTierUser() throw() = 0;

  /**
   * Creates the specified directory.
   *
   * @param requester The identity of the user requesting the creation of the
   * directory.
   * @param dirPath The absolute path of the directory.
   */
  virtual void createDirectory(
    const SecurityIdentity &requester,
    const std::string &dirPath) = 0;

  /**
   * Deletes the specified directory.
   *
   * @param requester The identity of the user requesting the deletion of the
   * directory.
   * @param dirPath The absolute path of the directory.
   */
  virtual void deleteDirectory(
    const SecurityIdentity &requester,
    const std::string &dirPath) = 0;

  /**
   * Gets the contents of the specified directory.
   *
   * @param requester The identity of the user requesting the contents of the
   * directory.
   * @param dirPath The absolute path of the directory.
   * @return An iterator over the contents of the directory.
   */
  virtual DirectoryIterator getDirectoryContents(
    const SecurityIdentity &requester,
    const std::string &dirPath) const = 0;

  /**
   * Sets the storage class of the specified directory to the specified value.
   *
   * @param requester The identity of the user requesting the setting of the
   * directory's storage class.
   * @param dirPath The absolute path of the directory.
   * @param storageClassName The name of the storage class.
   */
  virtual void setDirectoryStorageClass(
    const SecurityIdentity &requester,
    const std::string &dirPath,
    const std::string &storageClassName) = 0;

  /**
   * Clears the storage class of the specified directory.
   *
   * @param requester The identity of the user requesting the storage class of
   * the directory to be cleared.
   * @param dirPath The absolute path of the directory.
   */
  virtual void clearDirectoryStorageClass(
    const SecurityIdentity &requester,
    const std::string &dirPath) = 0;

  /**
   * Gets the storage class if of the specified directory if the directory has
   * one.
   *
   * @param requester The identity of the user requesting the storage class of
   * the directory.
   * @param dirPath The absolute path of the directory.
   * @return The name of the storage class if the directory has one, else an
   * empty string.
   */
  virtual std::string getDirectoryStorageClass(
    const SecurityIdentity &requester,
    const std::string &dirPath) const = 0;

  /**
   * Creates an archival job to asynchronously archive the specified source
   * files to the specified destination within the archive.
   *
   * If there is more than one source file then the destination must be a
   * directory.
   *
   * If there is only one source file then the destination can be either a file
   * or a directory.
   *
   * The storage class of the archived file will be inherited from its
   * destination directory.
   *
   * @param requester The identity of the user requesting the archival.
   * @param srcUrls List of one or more source files.
   * @param dst Destination file or directory within the archive namespace.
   */
  virtual void archive(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    const std::string &dst) = 0;

  /**
   * Returns all of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::map<TapePool, std::list<ArchivalJob> > getArchivalJobs(
    const SecurityIdentity &requester) = 0;

  /**
   * Returns the list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param tapePoolName The name of the tape pool.
   * @return The list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::list<ArchivalJob> getArchivalJobs(
    const SecurityIdentity &requester,
    const std::string &tapePoolName) = 0;

  /**
   * Creates a retrieval job to asynchronously retrieve the specified archived
   * files and copy them to the specified destination URL.
   *
   * If there is more than one archived file then the destination must be a
   * directory.
   *
   * If there is only one archived file then the destination can be either a
   * file or a directory.
   *
   * @param requester The identity of the user requesting the retrieval.
   * @param srcPaths The list of one of more archived files.
   * @param dstUrl The URL of the destination file or directory.
   */
  virtual void retrieve(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcPaths,
    const std::string &dstUrl) = 0;

  /**
   * Returns all of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::map<Tape, std::list<RetrievalJob> > getRetrievalJobs(
    const SecurityIdentity &requester) = 0;

  /**
   * Returns the list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param vid The volume identifier of the tape.
   * @return The list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::list<RetrievalJob> getRetrievalJobs(
    const SecurityIdentity &requester,
    const std::string &vid) = 0;

}; // class MiddleTierUser

} // namespace cta
