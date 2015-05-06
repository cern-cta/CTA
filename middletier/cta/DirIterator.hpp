#pragma once

#include "cta/DirEntry.hpp"

#include <list>

namespace cta {

/**
 * Instances of ths class should be used in the following way to iterate
 * through the contents of a directory.
 *
 * \code
 * DirIterator &itor = ...
 *
 * while(itor.hasMore())) {
 *   const DirEntry &entry = itor.next();
 *
 *   // Do something with entry
 * }
 * \endcode
 */
class DirIterator {
public:

  /**
   * Constructor.
   */
  DirIterator();

  /**
   * Constructor.
   *
   * @param entries The directory entries.
   */
  DirIterator(const std::list<DirEntry> &entries);

  /**
   * Destructor.
   */
  ~DirIterator() throw();

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
  const DirEntry next();

private:

  /**
   * The directory entries.
   */
  std::list<DirEntry> m_entries;

}; // DirIterator

} // namespace cta
