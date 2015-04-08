#pragma once

#include "cta/Mount.hpp"

#include <stdint.h>

namespace cta {

/**
 * Class representing an archival mount.
 */
class ArchivalMount: public Mount {
public:

  /**
   * Constructor.
   *
   * @param id The identifier of the mount.
   * @param vid The volume idenitifier of the tape to be mounted.
   * @param nbFilesOnTapeBeforeMount The number of files on the tape before the
   * mount.
   */
  ArchivalMount(const std::string &id, const std::string &vid,
    const uint64_t nbFilesOnTapeBeforeMount);

  /**
   * Destructor.
   */
  ~ArchivalMount() throw();

  /**
   * Returns the number of files on the tape before the mount.
   */
  uint64_t getNbFilesOnTapeBeforeMount() const throw();

private:

  /**
   * The number of files on the tape before the mount.
   */
  uint64_t m_nbFilesOnTapeBeforeMount;

}; // class ArchivalMount

} // namespace cta
