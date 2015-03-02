#pragma once

#include "FileSystemStorageClass.hpp"
#include "UserIdentity.hpp"

#include <list>
#include <map>
#include <string>

namespace cta {

/**
 * The storage classes used in the file system.
 */
class FileSystemStorageClasses {
public:

  /**
   * Creates the specified storage class.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param creator The identity of the user that created the storage class.
   * @param comment The comment describing the storage class.
   */
  void createStorageClass(
    const std::string &name,
    const uint8_t nbCopies,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   */
  void deleteStorageClass(const std::string &name);

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of storage classes in lexicographical order.
   */
  std::list<StorageClass> getStorageClasses() const;

  /**
   * Returns the specified storage class.
   *
   * @param name The name of the storage class.
   * @return The specified storage class.
   */
  const StorageClass &getStorageClass(const std::string &name) const;

  /**
   * Throws an exception if the specified storage class does not exist.
   *
   * @param name The name of the storage class.
   */
  void checkStorageClassExists(const std::string &name) const;

  /**
   * Increments the usage count of the specified storage class.
   *
   * @param name The name of the storage class.  If this parameter is set to the
   * empty string then this method does nothing.
   */
  void incStorageClassUsageCount(const std::string &name);

  /**
   * Decrements the usage count of the specified storage class.
   *
   * @param name The name of the storage class.  If this parameter is set to the
   * empty string then this method does nothing.
   */
  void decStorageClassUsageCount(const std::string &name);

  /**
   * Throws an exception if the specified storage class is use.
   *
   * @param name The name of the storage class.
   */
  void checkStorageClassIsNotInUse(const std::string &name) const;

protected:

  /**
   * The current mapping from storage class names to storage classes and their
   * usage counts.
   */
  std::map<std::string, FileSystemStorageClass> m_storageClasses;

  /**
   * Throws an exception if the specified storage class already exists.
   *
   * @param name The name of teh storage class.
   */
  void checkStorageClassDoesNotAlreadyExist(const std::string &name) const;

}; // class FileSystemStorageClasses

} // namespace cta
