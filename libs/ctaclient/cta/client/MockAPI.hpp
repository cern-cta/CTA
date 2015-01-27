#pragma once

#include "cta/client/API.hpp"

#include <map>

namespace cta {
namespace client {

/**
 * A mock entry point to the client API of the CERN Tape Archive project.
 */
class MockAPI: public API {
public:

  /**
   * Constructor.
   */
  MockAPI();

  /**
   * Destructor.
   */
  ~MockAPI() throw();

  /**
   * Creates the specified storage class.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   */
  void createStorageClass(
    const std::string &name,
    const uint8_t nbCopies);

  /**
   * Deletes the specified storage class.
   *
   * @param name The name of the storage class.
   */
  void deleteStorageClass(const std::string &name);

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @return The current list of storage classes in lexicographical order.
   */
  StorageClassList getStorageClasses() const;

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
  std::string archiveToTape(const std::list<std::string> &srcUrls,
    std::string dst);

private:

  /**
   * The current list of storage classes.
   */
  std::map<std::string, StorageClass> m_storageClasses;

  /**
   * Throws an exception if the specified storage class already exists.
   *
   * @param name The name of teh storage class.
   */
  void checkStorageClassDoesNotAlreadyExist(const std::string &name) const;

  /**
   * Throws an exception if the specified storage class does not exist.
   *
   * @param name The name of teh storage class.
   */
  void checkStorageClassExists(const std::string &name) const;

}; // class MockAPI

} // namespace client
} // namespace cta
