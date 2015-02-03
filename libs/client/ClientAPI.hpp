#pragma once

#include "DirectoryIterator.hpp"
#include "SecurityIdentity.hpp"
#include "StorageClass.hpp"
#include "UserIdentity.hpp"

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Abstract class that specifies the client ClientAPI of the CERN Tape Archive
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
   const  = 0;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   */
  virtual void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint8_t nbCopies) = 0;

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
   * @param requester The identity of the user requesting list.
   * @return The current list of storage classes in lexicographical order.
   */
  virtual std::list<StorageClass> getStorageClasses(
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
   * @param dirPath The absolute path of the directory.
   * @param storageClassName The name of the storage class.
   */
  virtual void setDirectoryStorageClass(const std::string &dirPath,
    const std::string &storageClassName) = 0;

  /**
   * Clears the storage class of the specified directory.
   *
   * @param dirPath The absolute path of the directory.
   * @param storageClassName The name of the storage class.
   */
  virtual void clearDirectoryStorageClass(const std::string &dirPath) = 0;

  /**
   * Gets the storage class if of the specified directory if the directory has
   * one.
   *
   * @param dirPath The absolute path of the directory.
   * @return The name of the storage class if the directory has one, else an
   * empty string.
   */
  virtual std::string getDirectoryStorageClass(const std::string &dirPath) = 0;

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
   * @return The identifier of the archive job.
   */
  virtual std::string archiveToTape(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    std::string dst) = 0;

}; // class ClientAPI

} // namespace cta
