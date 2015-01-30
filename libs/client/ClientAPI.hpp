#pragma once

#include "DirectoryIterator.hpp"
#include "StorageClass.hpp"

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Abstract class that specifies the client ClientAPI of the CERN Tape Archive project.
 */
class ClientAPI {
public:

  /**
   * Destructor.
   */
  virtual ~ClientAPI() throw() = 0;

  /**
   * Creates the specified storage class.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   */
  virtual void createStorageClass(
    const std::string &name,
    const uint8_t nbCopies) = 0;

  /**
   * Deletes the specified storage class.
   *
   * @param name The name of the storage class.
   */
  virtual void deleteStorageClass(const std::string &name) = 0;

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @return The current list of storage classes in lexicographical order.
   */
  virtual std::list<StorageClass> getStorageClasses() const = 0;

  /**
   * Creates the specified directory.
   *
   * @param dirPath The full path of the directory.
   */
  virtual void createDirectory(const std::string &dirPath) = 0;

  /**
   * Gets an iterator over the entries of the specified directory.
   *
   * @param dirPath The full path of the directory.
   * @return The iterator.
   */
  virtual DirectoryIterator getDirectoryIterator(
    const std::string &dirPath) const = 0;

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
   * @param srcUrls List of one or more source files.
   * @param dst Destination file or directory within the archive namespace.
   * @return The identifier of the archive job.
   */
  virtual std::string archiveToTape(const std::list<std::string> &srcUrls,
    std::string dst) = 0;

}; // class ClientAPI

} // namespace cta
