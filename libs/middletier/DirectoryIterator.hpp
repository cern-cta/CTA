#pragma once

#include "DirectoryEntry.hpp"

#include <list>

namespace cta {

/**
 * Instances of ths class should be used in the following way to iterate
 * through the contents of a directory.
 *
 * \code
 * DirectoryIterator &itor = ...
 *
 * while(itor.hasMore())) {
 *   const DirectoryEntry &entry = itor.next();
 *
 *   // Do something with entry
 * }
 * \endcode
 */
class DirectoryIterator {
public:

  /**
   * Constructor.
   */
  DirectoryIterator();

  /**
   * Constructor.
   *
   * @param entries The directory entries.
   */
  DirectoryIterator(const std::list<DirectoryEntry> &entries);

  /**
   * Destructor.
   */
  ~DirectoryIterator() throw();

  /**
   * Returns true if there are more directory entries.
   *
   * @return True if there are more directory entries.
   */
  bool hasMore();

  /**
   * Returns a reference to the next directory entry or throws an exception if
   * out of bounds.
   *
   * @return The next directory entry.
   */
  const DirectoryEntry next();

private:

  /**
   * The directory entries.
   */
  std::list<DirectoryEntry> m_entries;

}; // DirectoryIterator

} // namespace cta
