#pragma once

#include "ArchiveJob.hpp"
#include "DirectoryIterator.hpp"
#include "LogicalLibrary.hpp"
#include "MigrationRoute.hpp"
#include "SecurityIdentity.hpp"
#include "StorageClass.hpp"
#include "TapePool.hpp"
#include "UserIdentity.hpp"

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Abstract class that specifies the client API of the CERN Tape Archive
 * project.
 */
class ClientAPI {
public:

  /**
   * Destructor.
   */
  virtual ~ClientAPI() throw() = 0;

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the user requesting the creation of the
   * administrator.
   * @param adminUser The identity of the administrator.
   */
  virtual void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &adminUser) = 0;

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administrator.
   * @param adminUser The identity of the administrator.
   */
  virtual void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &adminUser) = 0;

  /**
   * Returns the current list of administrators.
   *
   * @param requester The identity of the user requesting the list.
   */
  virtual std::list<UserIdentity> getAdminUsers(const SecurityIdentity &requester)
   const = 0;

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the user requesting the creation of the
   * administration host.
   * @param adminHost The network name of the administration host.
   */
  virtual void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &adminHost) = 0;

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administration host.
   * @param adminHost The network name of the administration host.
   */
  virtual void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &adminHost) = 0;

  /**
   * Returns the current list of administration hosts.
   *
   * @param requester The identity of the user requesting the list.
   */
  virtual std::list<std::string> getAdminHosts(const SecurityIdentity &requester)
   const = 0;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param comment The comment describing the storage class.
   */
  virtual void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint8_t nbCopies,
    const std::string &comment) = 0;

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   */
  virtual void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of storage classes in lexicographical order.
   */
  virtual std::list<StorageClass> getStorageClasses(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the creation of the
   * tape pool.
   * @param name The name of the tape pool.
   * @param nbDrives The maximum number of drives that can be concurrently
   * assigned to this pool independent of whether they are archiving or
   * retrieving files.
   * @param nbPartialTapes The maximum number of tapes that can be partially
   * full at any moment in time.
   * @param comment The comment describing the tape pool.
   */
  virtual void createTapePool(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbDrives,
    const uint32_t nbPartialTapes,
    const std::string &comment) = 0;

  /**
   * Delete the tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape pool.
   * @param name The name of the tape pool.
   */
  virtual void deleteTapePool(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of tape pools in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tape pools in lexicographical order.
   */
  virtual std::list<TapePool> getTapePools(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates the specified migration route.
   *
   * @param requester The identity of the user requesting the creation of the
   * migration route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param comment The comment describing the migration route.
   */
  virtual void createMigrationRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint8_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified migration route.
   *
   * @param requester The identity of the user requesting the deletion of the
   * migration route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  virtual void deleteMigrationRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint8_t copyNb) = 0;

  /**
   * Gets the current list of migration routes.
   *
   * @param requester The identity of the user requesting the list.
   */
  virtual std::list<MigrationRoute> getMigrationRoutes(
    const SecurityIdentity &requester) const = 0;

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
   * Creates a logical library with the specified name and device group.
   *
   * @param requester The identity of the user requesting the creation of the
   * logical library.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  virtual void createLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name,
    const std::string &comment) = 0;

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * logical library.
   * @param name The name of the logical library.
   */
  virtual void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of libraries in lexicographical order.
   */
  virtual std::list<LogicalLibrary> getLogicalLibraries(
    const SecurityIdentity &requester) const = 0;

  /**
   * Archives the specified list of source files to the specified destination
   * within the archive namespace.
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
   * @return The string identifier of the archive job.
   */
  virtual std::string archive(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    const std::string &dst) = 0;

  /**
   * Gets the current list of archive jobs associated with the specified device
   * group.
   *
   * @param requester The identity of the user requesting the list.
   * @param tapePoolName The name of the tape pool.
   * @return The list of jobs sorted by creation time in ascending order
   * (oldest first).
   */
  virtual std::list<ArchiveJob> getArchiveJobs(
    const SecurityIdentity &requester,
    const std::string &tapePoolName) = 0;

}; // class ClientAPI

} // namespace cta
