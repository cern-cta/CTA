#pragma once

#include "cta/Mount.hpp"

#include <stdint.h>

namespace cta {

/**
 * Class representing a retrieval mount.
 */
class RetrievalMount: public Mount {
public:

  /**
   * Constructor.
   *
   * @param id The identifier of the mount.
   * @param vid The volume idenitifier of the tape to be mounted.
   */
  RetrievalMount(const std::string &id, const std::string &vid);

  /**
   * Destructor.
   */
  ~RetrievalMount() throw();

}; // class RetrievalMount

} // namespace cta
