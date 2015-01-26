#pragma once

#include <list>
#include <stdint.h>
#include <string>

namespace cta {
namespace client {

/**
 * A singleton class representing the entry point to the client API of the CERN
 * Tape Archive project.
 */
class API {
public:

  /**
   * Returns a pointer to the entry point of the client API.
   *
   * @return A pointer to the entry point of the client API.
   */
  static API *instance();

  /**
   * Creates the specified storage class.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   */
  virtual void createStorageClass(
    const std::string &name,
    const uint8_t nbCopies);

  /**
   * Deletes the specified storage class.
   *
   * @param name The name of the storage class.
   */
  virtual void deleteStorageClass(const std::string &name);

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @return The current list of storage classes in lexicographical order.
   */
  virtual std::list<std::string> getStorageClasses();

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
    std::string dst);

private:

  /**
   * Constructor.
   */
  API();

  /**
   * Destructor.
   */
  ~API();

  /**
   * Pointer to the entry point of the client API.
   */
  static API *s_instance;

}; // class API

} // namespace client
} // namespace cta
