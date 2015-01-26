#pragma once

#include "cta/filesystem/DirectoryEntry.hpp"

namespace cta {
namespace filesystem {

/**
 * Instances of ths class should be used in the following way to iterate
 * through the contents of a directory.
 *
 * \code
 * DirectoryConstIterator &itor = ...
 *
 * while(itor.hasMore())) {
 *   const DirectoryEntry &entry = itor.next();
 *
 *   // Do something with entry
 * }
 * \endcode
 */
class DirectoryConstIterator {
public:

  /**
   * Returns true if there are more directory entries.
   *
   * @return True if there are more directory entries.
   */
  virtual bool hasMore() = 0;

  /**
   * Returns a reference to the next directory entry or throws an exception if
   * out of bounds.
   *
   * @return The next directory entry.
   */
  virtual const DirectoryEntry &next() = 0;

}; // DirectoryConstIterator

} // namespace filesystem
} // namespace cta
