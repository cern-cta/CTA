#pragma once

#include "MiddleTierAdmin.hpp"
#include "MockMiddleTierDatabase.hpp"

namespace cta {

/**
 * The administration API of the mock middle-tier.
 */
class MockMiddleTierAdmin: public MiddleTierAdmin {
public:

  /**
   * Constructor.
   *
   * @param db The database of the mock middle-tier.
   */
  MockMiddleTierAdmin(MockMiddleTierDatabase &db);

  /**
   * Destructor.
   */
  ~MockMiddleTierAdmin() throw();

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the user requesting the creation of the
   * administrator.
   * @param user The identity of the administrator.
   * @param comment The comment describing te administator.
   */
  void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment);

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administrator.
   * @param user The identity of the administrator.
   */
  void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user);

  /**
   * Returns the current list of administrators in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administrators in lexicographical order.
   */
  std::list<AdminUser> getAdminUsers(const SecurityIdentity &requester) const;

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the user requesting the creation of the
   * administration host.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment);

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administration host.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName);

  /**
   * Returns the current list of administration hosts.
   *
   * @param requester The identity of the user requesting the list.
   */
  std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester) const;

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
  void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint8_t nbCopies,
    const std::string &comment);

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   */
  void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of storage classes in lexicographical order.
   */
  std::list<StorageClass> getStorageClasses(
    const SecurityIdentity &requester) const;

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
  void createTapePool(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbDrives,
    const uint32_t nbPartialTapes,
    const std::string &comment);

  /**
   * Delete the tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape pool.
   * @param name The name of the tape pool.
   */
  void deleteTapePool(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Gets the current list of tape pools in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tape pools in lexicographical order.
   */
  std::list<TapePool> getTapePools(
    const SecurityIdentity &requester) const;

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
  void createMigrationRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint8_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment);

  /**
   * Deletes the specified migration route.
   *
   * @param requester The identity of the user requesting the deletion of the
   * migration route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  void deleteMigrationRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint8_t copyNb);

  /**
   * Gets the current list of migration routes.
   *
   * @param requester The identity of the user requesting the list.
   */
  std::list<MigrationRoute> getMigrationRoutes(
    const SecurityIdentity &requester) const;

  /**
   * Creates a logical library with the specified name and device group.
   *
   * @param requester The identity of the user requesting the creation of the
   * logical library.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  void createLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name,
    const std::string &comment);

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * logical library.
   * @param name The name of the logical library.
   */
  void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of libraries in lexicographical order.
   */
  std::list<LogicalLibrary> getLogicalLibraries(
    const SecurityIdentity &requester) const;

protected:

  /**
   * Throws an exception if the specified administrator already exists.
   *
   * @param user The identity of the administrator.
   */
  void checkAdminUserDoesNotAlreadyExist(const UserIdentity &user) const;

  /**
   * Throws an exception if the specified administration host already exists.
   *
   * @param hostName The network name of the administration host.
   */
  void checkAdminHostDoesNotAlreadyExist(const std::string &hostName) const;

  /**
   * Gets the file system node corresponding to the specified path.
   *
   * @path The path.
   * @return The corresponding file system node.
   */
  FileSystemNode &getFileSystemNode(const std::string &path);

  /**
   * Gets the file system node corresponding to the specified path.
   *
   * @path The path.
   * @return The corresponding file system node.
   */
  const FileSystemNode &getFileSystemNode(const std::string &path) const;

  /**
   * Throws an exception if the specified tape pool already exixts.
   *
   * @param name The name of the tape pool.
   */
  void checkTapePoolDoesNotAlreadyExist(const std::string &name) const;

  /**
   * Throws an exception if the specified tape pool is in use.
   *
   * @param name The name of the tape pool.
   */
  void checkTapePoolIsNotInUse(const std::string &name) const;

  /**
   * Throws an exception if the specified storage class is used in a migration
   * route.
   *
   * @param name The name of the storage class.
   */
  void checkStorageClassIsNotInAMigrationRoute(const std::string &name) const;

  /**
   * Returns true if the specified absolute path is that of an existing
   * directory within the archive namepsace.
   *
   * @param path The specified absolute path.
   * @return True if the specified absolute path is that of an existing
   * directory within the archive namepsace.
   */
  bool isAnExistingDirectory(const std::string &path) const throw();

  /**
   * Archives the specified list of source files to the specified destination
   * directory within the archive namespace.
   *
   * The list of source files should contain at least one file.
   *
   * The storage class of the archived file will be inherited from its
   * destination directory.
   *
   * @param requester The identity of the user requesting the archival.
   * @param srcUrls List of one or more source files.
   * @param dstDir Destination directory within the archive namespace.
   * @return The string identifier of the archive job.
   */
  std::string archiveToDirectory(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    const std::string &dstDir);

  /**
   * Archives the specified list of source files to the specified destination
   * file within the archive namespace.
   *
   * The list of source files should contain one and only one file.
   *
   * The storage class of the archived file will be inherited from its
   * destination directory.
   *
   * @param requester The identity of the user requesting the archival.
   * @param srcUrls List of one or more source files.
   * @param dstFile Destination file within the archive namespace.
   * @return The string identifier of the archive job.
   */
  std::string archiveToFile(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    const std::string &dstFile);

  /**
   * Throws an exception if the specified requester is not authorised to archive
   * a file to the specified destination directory within the archive namespace.
   *
   * @param requester The identity of the user requesting the archival.
   * @param dstDir Destination directory within the archive namespace.
   */
  void checkUserIsAuthorisedToArchive(
    const SecurityIdentity &user,
    const FileSystemNode &dstDir);

  /**
   * Throws an exception if the specified logical library already exists.
   *
   * @param name The name of the logical library.
   */
  void checkLogicalLibraryDoesNotAlreadyExist(const std::string &name) const;

  /**
   * The database of the mock middle-tier.
   */
  MockMiddleTierDatabase &m_db;

}; // class MockMiddleTierAdmin

} // namespace cta
